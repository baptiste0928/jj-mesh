//! Stall detection on transfer streams.
//!
//! A transfer can legitimately run for hours (a large clone over a slow
//! link), so it has no overall deadline: [`Idle`] only fails a read or a
//! write that waits without progress for its timeout. Local work between
//! operations does not count against it, but the peer's does.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{Instant, Sleep},
};

/// A stream failing with [`io::ErrorKind::TimedOut`] when an operation waits
/// without progress for `timeout`. Meant for one direction: a read and a
/// write waiting at once would share the clock, and so would an operation
/// cancelled mid-wait and the next one.
pub struct Idle<S> {
    inner: S,
    timeout: Duration,
    stall: Pin<Box<Sleep>>,
    /// Whether an operation is waiting, so the clock starts when it does.
    waiting: bool,
}

impl<S> Idle<S> {
    /// Wraps `inner`, failing operations stalled for `timeout`.
    pub fn new(inner: S, timeout: Duration) -> Self {
        Idle {
            inner,
            timeout,
            stall: Box::pin(tokio::time::sleep(timeout)),
            waiting: false,
        }
    }

    /// Passes a poll's progress through, or fails it once it waited for
    /// the whole timeout.
    fn track<T>(&mut self, cx: &mut Context<'_>, poll: Poll<io::Result<T>>) -> Poll<io::Result<T>> {
        if poll.is_ready() {
            self.waiting = false;
            return poll;
        }
        if !self.waiting {
            self.waiting = true;
            self.stall.as_mut().reset(Instant::now() + self.timeout);
        }
        match self.stall.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("no progress for {}s", self.timeout.as_secs()),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Idle<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        this.track(cx, poll)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Idle<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.track(cx, poll)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_flush(cx);
        this.track(cx, poll)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.track(cx, poll)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    /// A stalled read fails after the timeout, but time spent between
    /// operations does not count.
    #[tokio::test(start_paused = true)]
    async fn fails_only_waiting_operations() {
        let (client, mut server) = tokio::io::duplex(64);
        let mut idle = Idle::new(client, Duration::from_secs(10));

        tokio::time::sleep(Duration::from_secs(60)).await;
        server.write_all(b"x").await.unwrap();
        let mut byte = [0u8; 1];
        idle.read_exact(&mut byte).await.unwrap();

        let err = idle.read_exact(&mut byte).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }
}
