use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{self, ErrorKind};
use std::time::{Duration, Instant};

#[cfg(unix)]
mod os {
    use crate::posix;
    use std::cmp::min;
    use std::fs::File;
    use std::io::{self, Read, Write};
    use std::os::unix::io::AsRawFd;
    use std::time::Instant;

    fn millisecs_until(t: Instant) -> u32 {
        let now = Instant::now();
        if t <= now {
            return 0;
        }
        let diff = t - now;
        (diff.as_secs() * 1000) as u32 + diff.subsec_millis()
    }

    fn poll3(
        fin: Option<&File>,
        fout: Option<&File>,
        ferr: Option<&File>,
        deadline: Option<Instant>,
    ) -> io::Result<(bool, bool, bool)> {
        fn to_poll(f: Option<&File>, for_read: bool) -> posix::PollFd {
            let optfd = f.map(File::as_raw_fd);
            let events = if for_read {
                posix::POLLIN
            } else {
                posix::POLLOUT
            };
            posix::PollFd::new(optfd, events)
        }

        let mut fds = [
            to_poll(fin, false),
            to_poll(fout, true),
            to_poll(ferr, true),
        ];
        posix::poll(&mut fds, deadline.map(millisecs_until))?;

        Ok((
            fds[0].test(posix::POLLOUT | posix::POLLHUP),
            fds[1].test(posix::POLLIN | posix::POLLHUP),
            fds[2].test(posix::POLLIN | posix::POLLHUP),
        ))
    }

    #[derive(Debug)]
    pub struct Communicator {
        stdin: Option<File>,
        stdout: Option<File>,
        stderr: Option<File>,
        input_data: Vec<u8>,
        input_pos: usize,
    }

    impl Communicator {
        pub fn new(
            stdin: Option<File>,
            stdout: Option<File>,
            stderr: Option<File>,
            input_data: Option<Vec<u8>>,
        ) -> Communicator {
            let input_data = input_data.unwrap_or_else(Vec::new);
            Communicator {
                stdin,
                stdout,
                stderr,
                input_data,
                input_pos: 0,
            }
        }

        fn do_read(
            source_ref: &mut Option<&File>,
            dest: &mut Vec<u8>,
            size_limit: Option<usize>,
            total_read: usize,
        ) -> io::Result<()> {
            let mut buf = &mut [0u8; 4096][..];
            if let Some(size_limit) = size_limit {
                if total_read >= size_limit {
                    return Ok(());
                }
                if size_limit - total_read < buf.len() {
                    buf = &mut buf[0..size_limit - total_read];
                }
            }
            let n = source_ref.unwrap().read(buf)?;
            if n != 0 {
                dest.extend_from_slice(&mut buf[..n]);
            } else {
                *source_ref = None;
            }
            Ok(())
        }

        pub fn read_into(
            &mut self,
            deadline: Option<Instant>,
            size_limit: Option<usize>,
            mut outvec: &mut Vec<u8>,
            mut errvec: &mut Vec<u8>,
        ) -> io::Result<()> {
            // Note: chunk size for writing must be smaller than the pipe buffer
            // size.  A large enough write to a pipe deadlocks despite polling.
            const WRITE_SIZE: usize = 4096;

            let mut stdout_ref = self.stdout.as_ref();
            let mut stderr_ref = self.stderr.as_ref();

            loop {
                if let Some(size_limit) = size_limit {
                    if outvec.len() + errvec.len() >= size_limit {
                        break;
                    }
                }

                if let (None, None, None) = (self.stdin.as_ref(), stdout_ref, stderr_ref) {
                    // When no stream remains, we are done.
                    break;
                }

                let (in_ready, out_ready, err_ready) =
                    poll3(self.stdin.as_ref(), stdout_ref, stderr_ref, deadline)?;
                if !in_ready && !out_ready && !err_ready {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "timeout"));
                }
                if in_ready {
                    let input = &self.input_data[self.input_pos..];
                    let chunk = &input[..min(WRITE_SIZE, input.len())];
                    let n = self.stdin.as_ref().unwrap().write(chunk)?;
                    self.input_pos += n;
                    if self.input_pos == self.input_data.len() {
                        // close stdin when done writing, so the child receives EOF
                        self.stdin.take();
                        // free the input data vector, we don't need it any more
                        self.input_data.clear();
                        self.input_data.shrink_to_fit();
                    }
                }
                if out_ready {
                    let total = outvec.len() + errvec.len();
                    Communicator::do_read(&mut stdout_ref, &mut outvec, size_limit, total)?;
                }
                if err_ready {
                    let total = outvec.len() + errvec.len();
                    Communicator::do_read(&mut stderr_ref, &mut errvec, size_limit, total)?;
                }
            }

            Ok(())
        }

        pub fn read(
            &mut self,
            deadline: Option<Instant>,
            size_limit: Option<usize>,
        ) -> (Option<io::Error>, (Option<Vec<u8>>, Option<Vec<u8>>)) {
            let mut outvec = Vec::<u8>::new();
            let mut errvec = Vec::<u8>::new();

            let result = self.read_into(deadline, size_limit, &mut outvec, &mut errvec);
            let output = (
                self.stdout.as_ref().map(|_| outvec),
                self.stderr.as_ref().map(|_| errvec),
            );
            match result {
                Ok(()) => (None, output),
                Err(e) => (Some(e), output),
            }
        }
    }
}

#[cfg(windows)]
mod os {
    use std::fs::File;
    use std::io::{self, Read, Write};
    use std::mem;
    use std::sync::mpsc::{self, RecvTimeoutError, SyncSender};
    use std::thread;
    use std::time::Instant;

    #[derive(Debug, Copy, Clone)]
    enum StreamIdent {
        In = 1 << 0,
        Out = 1 << 1,
        Err = 1 << 2,
    }

    enum Payload {
        Data(Vec<u8>),
        EOF,
        Err(io::Error),
    }

    // Messages exchanged between Communicator's helper threads.
    type Message = (StreamIdent, Payload);

    fn read_and_transmit(mut outfile: File, ident: StreamIdent, sink: SyncSender<Message>) {
        let mut chunk = [0u8; 4096];
        // Note: failing to sending to the sink means we are done.  It will
        // fail if the main thread drops the Communicator (and with it the
        // receiver) prematurely e.g. because a limit was reached or another
        // helper encountered an IO error.
        loop {
            match outfile.read(&mut chunk) {
                Ok(0) => {
                    let _ = sink.send((ident, Payload::EOF));
                    break;
                }
                Ok(nread) => {
                    if let Err(_) = sink.send((ident, Payload::Data(chunk[..nread].to_vec()))) {
                        break;
                    }
                }
                Err(e) => {
                    let _ = sink.send((ident, Payload::Err(e)));
                    break;
                }
            }
        }
    }

    fn spawn_curried<T: Send + 'static>(f: impl FnOnce(T) + Send + 'static, arg: T) {
        thread::spawn(move || f(arg));
    }

    #[derive(Debug)]
    pub struct Communicator {
        rx: mpsc::Receiver<Message>,
        helper_set: u8,
        requested_streams: u8,
        leftover: Option<(StreamIdent, Vec<u8>)>,
    }

    struct Timeout;

    impl Communicator {
        pub fn new(
            stdin: Option<File>,
            stdout: Option<File>,
            stderr: Option<File>,
            input_data: Option<Vec<u8>>,
        ) -> Communicator {
            let mut helper_set = 0u8;
            let mut requested_streams = 0u8;

            let read_stdout = stdout.map(|stdout| {
                helper_set |= StreamIdent::Out as u8;
                requested_streams |= StreamIdent::Out as u8;
                |tx| read_and_transmit(stdout, StreamIdent::Out, tx)
            });
            let read_stderr = stderr.map(|stderr| {
                helper_set |= StreamIdent::Err as u8;
                requested_streams |= StreamIdent::Err as u8;
                |tx| read_and_transmit(stderr, StreamIdent::Err, tx)
            });
            let write_stdin = stdin.map(|mut stdin| {
                let input_data = input_data.expect("must provide input to redirected stdin");
                helper_set |= StreamIdent::In as u8;
                move |tx: SyncSender<_>| match stdin.write_all(&input_data) {
                    Ok(()) => mem::drop(tx.send((StreamIdent::In, Payload::EOF))),
                    Err(e) => mem::drop(tx.send((StreamIdent::In, Payload::Err(e)))),
                }
            });

            let (tx, rx) = mpsc::sync_channel(1);

            read_stdout.map(|f| spawn_curried(f, tx.clone()));
            read_stderr.map(|f| spawn_curried(f, tx.clone()));
            write_stdin.map(|f| spawn_curried(f, tx.clone()));

            Communicator {
                rx,
                helper_set,
                requested_streams,
                leftover: None,
            }
        }

        fn recv_until(&self, deadline: Option<Instant>) -> Result<Message, Timeout> {
            if let Some(deadline) = deadline {
                let now = Instant::now();
                if now >= deadline {
                    return Err(Timeout);
                }
                match self.rx.recv_timeout(deadline - now) {
                    Ok(message) => Ok(message),
                    Err(RecvTimeoutError::Timeout) => Err(Timeout),
                    // should never be disconnected, the helper threads always
                    // announce their exit
                    Err(RecvTimeoutError::Disconnected) => unreachable!(),
                }
            } else {
                Ok(self.rx.recv().unwrap())
            }
        }

        fn as_options(
            &self,
            outvec: Vec<u8>,
            errvec: Vec<u8>,
        ) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
            let (mut o, mut e) = (None, None);
            if self.requested_streams & StreamIdent::Out as u8 != 0 {
                o = Some(outvec);
            } else {
                assert!(outvec.len() == 0);
            }
            if self.requested_streams & StreamIdent::Err as u8 != 0 {
                e = Some(errvec);
            } else {
                assert!(errvec.len() == 0);
            }
            (o, e)
        }

        pub fn read(
            &mut self,
            deadline: Option<Instant>,
            size_limit: Option<usize>,
        ) -> (Option<io::Error>, (Option<Vec<u8>>, Option<Vec<u8>>)) {
            // Create both vectors immediately.  This doesn't allocate, and if
            // one of those is not needed, it just won't get resized.
            let mut outvec = vec![];
            let mut errvec = vec![];

            let mut grow_result =
                |ident, mut data: &[u8], leftover: &mut Option<(StreamIdent, Vec<u8>)>| {
                    if let Some(size_limit) = size_limit {
                        let total_read = outvec.len() + errvec.len();
                        if total_read >= size_limit {
                            return false;
                        }
                        let remaining = size_limit - total_read;
                        if data.len() > remaining {
                            *leftover = Some((ident, data[remaining..].to_vec()));
                            data = &data[..remaining];
                        }
                    }
                    let destvec = match ident {
                        StreamIdent::Out => &mut outvec,
                        StreamIdent::Err => &mut errvec,
                        StreamIdent::In => unreachable!(),
                    };
                    destvec.extend_from_slice(data);
                    if let Some(size_limit) = size_limit {
                        if outvec.len() + errvec.len() >= size_limit {
                            return false;
                        }
                    }
                    return true;
                };

            if let Some((ident, data)) = self.leftover.take() {
                if !grow_result(ident, &data, &mut self.leftover) {
                    return (None, self.as_options(outvec, errvec));
                }
            }

            while self.helper_set != 0 {
                match self.recv_until(deadline) {
                    Ok((ident, Payload::EOF)) => {
                        self.helper_set &= !(ident as u8);
                        continue;
                    }
                    Ok((ident, Payload::Data(data))) => {
                        assert!(data.len() != 0);
                        if !grow_result(ident, &data, &mut self.leftover) {
                            break;
                        }
                    }
                    Ok((_ident, Payload::Err(e))) => {
                        return (Some(e), self.as_options(outvec, errvec))
                    }
                    Err(Timeout) => {
                        return (
                            Some(io::Error::new(io::ErrorKind::TimedOut, "timeout")),
                            self.as_options(outvec, errvec),
                        )
                    }
                }
            }

            (None, self.as_options(outvec, errvec))
        }
    }
}

/// Deadlock-free communication with the subprocess.
///
/// Normally care must be taken to avoid deadlock when communicating to a
/// subprocess that both expects input and provides output.  This
/// implementation avoids deadlock by reading from and writing to the
/// subprocess in parallel.  On Unix-like systems this is achieved using
/// `poll()`, and on Windows using threads.
#[derive(Debug)]
pub struct Communicator {
    inner: os::Communicator,
    size_limit: Option<usize>,
    time_limit: Option<Duration>,
}

impl Communicator {
    fn new(
        stdin: Option<File>,
        stdout: Option<File>,
        stderr: Option<File>,
        input_data: Option<Vec<u8>>,
    ) -> Communicator {
        Communicator {
            inner: os::Communicator::new(stdin, stdout, stderr, input_data),
            size_limit: None,
            time_limit: None,
        }
    }

    /// Read the data from the subprocess.
    ///
    /// This will write the input data to the subprocess and read its output
    /// and error in parallel, taking care to avoid deadlocks.  The output and
    /// error are returned as pairs of `Option<Vec>`, which can be `None` if
    /// the corresponding stream has not been specified as
    /// `Redirection::Pipe`.
    ///
    /// By default `read()` will read all requested data.
    ///
    /// If `limit_time` has been called with a non-`None` time limit, the
    /// method will read for no more than the specified duration.  In case of
    /// timeout, an `io::Error` of kind `io::ErrorKind::TimedOut` is returned.
    /// Communication may be resumed after the timeout by calling `read()`
    /// again.
    ///
    /// If `limit_size` has been called with a non-`None` time limit, the
    /// method will return no more than the specified amount of bytes in the
    /// two vectors combined.  (It might internally read a bit more from the
    /// subprocess.)  Subsequent data may be read by calling `read()` again.
    /// The primary use case for this method is preventing a rogue subprocess
    /// from breaking the caller by spending all its memory.
    pub fn read(
        &mut self,
    ) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>), CommunicateError> {
        let deadline = self.time_limit.map(|timeout| Instant::now() + timeout);
        match self.inner.read(deadline, self.size_limit) {
            (None, capture) => Ok(capture),
            (Some(error), capture) => Err(CommunicateError { error, capture }),
        }
    }

    /// Limit the amount of data the next `read()` will read from the
    /// subprocess.
    pub fn limit_size(mut self, size: usize) -> Communicator {
        self.size_limit = Some(size);
        self
    }

    /// Limit the amount of time the next `read()` will spend reading from the
    /// subprocess.
    pub fn limit_time(mut self, time: Duration) -> Communicator {
        self.time_limit = Some(time);
        self
    }

    // XXX
    // * read_timeout
    // * read_string
}

pub fn communicate(
    stdin: Option<File>,
    stdout: Option<File>,
    stderr: Option<File>,
    input_data: Option<Vec<u8>>,
) -> Communicator {
    if stdin.is_some() {
        input_data
            .as_ref()
            .expect("must provide input to redirected stdin");
    } else {
        assert!(
            input_data.as_ref().is_none(),
            "cannot provide input to non-redirected stdin"
        );
    }
    Communicator::new(stdin, stdout, stderr, input_data)
}

/// Error during communication.
///
/// This error encapsulates the underlying `io::Error`, but also provides the
/// data captured before the error was encountered.
#[derive(Debug)]
pub struct CommunicateError {
    /// The underlying `io::Error`.
    pub error: io::Error,
    /// The data captured before the error was encountered.
    pub capture: (Option<Vec<u8>>, Option<Vec<u8>>),
}

impl CommunicateError {
    /// Returns the corresponding `ErrorKind` for this error.
    pub fn kind(&self) -> ErrorKind {
        self.error.kind()
    }
}

impl Error for CommunicateError {
    fn description(&self) -> &str {
        self.error.description()
    }

    fn cause(&self) -> Option<&dyn Error> {
        self.error.source()
    }
}

impl fmt::Display for CommunicateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.error.fmt(f)
    }
}
