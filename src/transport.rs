//! The transport seam: where bytes travel between a client and the daemon.
//!
//! Everything above this module speaks newline-delimited JSON over an opaque
//! bidirectional stream and does not care what carries it. What differs per
//! platform lives here and only here:
//!
//! - **Unix**: a Unix domain socket inside the 0700 data directory, so access
//!   control is the directory's. The stale socket file left by a hard kill is
//!   removed before binding — the daemon lock, not the file, is what proves
//!   liveness.
//! - **Windows**: a named pipe, `\\.\pipe\lum-<data-dir>`, since pipe names
//!   are global rather than filesystem-scoped. The data directory is encoded
//!   into the name so two `LUM_DATA_DIR`s get two daemons, exactly as two
//!   socket paths do on Unix.
//!
//! Windows support is experimental: it compiles and its tests run in CI, but
//! no release binaries are published yet and the pipe uses the default ACL —
//! see the tracking issue (#27) for what remains.

use std::io;

use anyhow::{Context, Result};

use crate::config::Config;

/// The endpoint's name, for connection calls and error messages. On Unix this
/// is the socket path; on Windows the pipe name.
pub fn endpoint_name(config: &Config) -> String {
    imp::endpoint_name(config)
}

/// Whether a failed connect means "no daemon is listening" — the condition
/// that makes a client spawn one — as opposed to a daemon that exists but is
/// mid-handshake, which is a condition to retry without spawning.
pub fn is_unavailable(error: &io::Error) -> bool {
    imp::is_unavailable(error)
}

pub use imp::{connect, listen, Listener, Stream};

#[cfg(unix)]
mod imp {
    use super::*;
    use tokio::net::{UnixListener, UnixStream};

    pub type Stream = UnixStream;

    pub struct Listener(UnixListener);

    impl Listener {
        pub async fn accept(&mut self) -> io::Result<Stream> {
            self.0.accept().await.map(|(stream, _)| stream)
        }
    }

    pub fn endpoint_name(config: &Config) -> String {
        config.socket_path().to_string_lossy().into_owned()
    }

    pub fn listen(config: &Config) -> Result<Listener> {
        let socket = config.socket_path();
        // A socket file outliving its process is normal after a hard kill,
        // and bind fails on an existing path regardless of whether anyone is
        // behind it. The daemon lock is what actually proves nobody is.
        let _ = std::fs::remove_file(&socket);
        let listener =
            UnixListener::bind(&socket).with_context(|| format!("binding {}", socket.display()))?;
        let mut permissions = std::fs::metadata(&socket)?.permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o600);
        std::fs::set_permissions(&socket, permissions)?;
        Ok(Listener(listener))
    }

    pub async fn connect(config: &Config) -> io::Result<Stream> {
        UnixStream::connect(config.socket_path()).await
    }

    /// Remove the endpoint at shutdown. Only meaningful where it is a file.
    pub fn cleanup(config: &Config) {
        let _ = std::fs::remove_file(config.socket_path());
    }

    pub fn is_unavailable(error: &io::Error) -> bool {
        matches!(
            error.kind(),
            io::ErrorKind::NotFound
                | io::ErrorKind::ConnectionRefused
                | io::ErrorKind::AddrNotAvailable
        )
    }
}

#[cfg(windows)]
mod imp {
    use super::*;
    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions};

    /// One type for both directions. A named-pipe server end and client end
    /// are different types in tokio, and everything above this module wants
    /// one `Stream`; boxing the client side unifies them for the cost of a
    /// vtable hop per read, which does not measure against a pipe hop.
    pub enum Stream {
        Server(NamedPipeServer),
        Client(tokio::net::windows::named_pipe::NamedPipeClient),
    }

    impl tokio::io::AsyncRead for Stream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            match self.get_mut() {
                Stream::Server(s) => std::pin::Pin::new(s).poll_read(cx, buf),
                Stream::Client(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            }
        }
    }

    impl tokio::io::AsyncWrite for Stream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            match self.get_mut() {
                Stream::Server(s) => std::pin::Pin::new(s).poll_write(cx, buf),
                Stream::Client(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            }
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            match self.get_mut() {
                Stream::Server(s) => std::pin::Pin::new(s).poll_flush(cx),
                Stream::Client(s) => std::pin::Pin::new(s).poll_flush(cx),
            }
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            match self.get_mut() {
                Stream::Server(s) => std::pin::Pin::new(s).poll_shutdown(cx),
                Stream::Client(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            }
        }
    }

    pub struct Listener {
        name: String,
        /// The pipe instance waiting for the next client. Created eagerly so
        /// there is always an instance to connect to — the named-pipe
        /// equivalent of a listen backlog of one.
        next: NamedPipeServer,
    }

    impl Listener {
        pub async fn accept(&mut self) -> io::Result<Stream> {
            self.next.connect().await?;
            let connected =
                std::mem::replace(&mut self.next, ServerOptions::new().create(&self.name)?);
            Ok(Stream::Server(connected))
        }
    }

    pub fn endpoint_name(config: &Config) -> String {
        // The data directory, encoded into a pipe name. Deterministic and
        // reproducible from Lua and shell too, which a hash would not be:
        // path separators and the drive colon become dashes.
        let sanitized: String = config
            .data_dir
            .to_string_lossy()
            .chars()
            .map(|c| if matches!(c, '\\' | '/' | ':') { '-' } else { c })
            .collect();
        format!(r"\\.\pipe\lum-{sanitized}")
    }

    pub fn listen(config: &Config) -> Result<Listener> {
        let name = endpoint_name(config);
        let next = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&name)
            .with_context(|| format!("creating {name}"))?;
        Ok(Listener { name, next })
    }

    pub async fn connect(config: &Config) -> io::Result<Stream> {
        let name = endpoint_name(config);
        // ERROR_PIPE_BUSY means a daemon exists but every instance is mid
        // accept; that is a retry, never a reason to spawn a second daemon.
        for _ in 0..50 {
            match ClientOptions::new().open(&name) {
                Ok(client) => return Ok(Stream::Client(client)),
                Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(io::ErrorKind::TimedOut, "the lum pipe stayed busy"))
    }

    pub fn cleanup(_config: &Config) {
        // Named pipes vanish with their last handle; there is no file.
    }

    const ERROR_PIPE_BUSY: i32 = 231;
    const ERROR_FILE_NOT_FOUND: i32 = 2;

    pub fn is_unavailable(error: &io::Error) -> bool {
        error.kind() == io::ErrorKind::NotFound
            || error.raw_os_error() == Some(ERROR_FILE_NOT_FOUND)
    }
}

/// Remove the endpoint at shutdown, where it is a thing that persists.
pub fn cleanup(config: &Config) {
    imp::cleanup(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    fn test_config(dir: &Path) -> Config {
        Config {
            data_dir: dir.to_path_buf(),
            idle_timeout: crate::config::DEFAULT_IDLE_TIMEOUT,
            startup_timeout: crate::config::DEFAULT_STARTUP_TIMEOUT,
            embed_batch: crate::config::DEFAULT_EMBED_BATCH,
            embed_token_budget: crate::config::DEFAULT_TOKEN_BUDGET,
            embed_threads: None,
            exclude_dirs: Default::default(),
            model: crate::config::Model::Standard,
        }
    }

    #[tokio::test]
    async fn a_line_survives_the_round_trip() {
        // The one test that exercises the platform's actual transport — on
        // Unix locally, on a named pipe in the Windows CI job. Everything
        // above this module is platform-independent by construction, so this
        // is the seam where a Windows breakage would have to show up.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        let mut listener = listen(&config).expect("listening");
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accepting");
            let (reader, mut writer) = tokio::io::split(stream);
            let mut lines = BufReader::new(reader).lines();
            let line = lines.next_line().await.unwrap().unwrap();
            writer.write_all(format!("echo: {line}\n").as_bytes()).await.unwrap();
        });

        let stream = connect(&config).await.expect("connecting");
        let (reader, mut writer) = tokio::io::split(stream);
        writer.write_all(b"{\"id\":1,\"op\":\"ping\"}\n").await.unwrap();
        let mut lines = BufReader::new(reader).lines();
        let reply = lines.next_line().await.unwrap().unwrap();
        assert_eq!(reply, "echo: {\"id\":1,\"op\":\"ping\"}");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn connecting_to_nothing_reads_as_unavailable() {
        // This is the condition that makes a client spawn a daemon; if it
        // misreads on some platform, every command spawns nothing and times
        // out, or spawns duplicates.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        // Not `expect_err`: that requires the success type to be Debug, and
        // the Windows Stream wraps tokio types this test has no business
        // constraining.
        let error = match connect(&config).await {
            Ok(_) => panic!("connected to an endpoint nothing is serving"),
            Err(error) => error,
        };
        assert!(is_unavailable(&error), "unexpected error kind: {error:?}");
    }
}
