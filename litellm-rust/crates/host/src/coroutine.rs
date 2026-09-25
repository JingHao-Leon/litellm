//! Async coroutines with resume arguments on stable Rust.
//!
//! A [`Coroutine`] is a suspended computation that hands values out and takes answers
//! back in, the shape of nightly [`std::ops::Coroutine`] and of `genawaiter`'s `Gen`:
//!
//! | here                                   | `std::ops::Coroutine`         | `genawaiter`          |
//! |----------------------------------------|-------------------------------|-----------------------|
//! | `Coroutine::new(\|co\| async { .. })`  | `#[coroutine] \|arg\| { .. }` | `Gen::new(\|co\| ..)` |
//! | `co.yield_(value).await`               | `yield value`                 | `co.yield_(value)`    |
//! | `coroutine.resume(arg).await`          | `coroutine.resume(arg)`       | `gen.resume_with(arg)`|
//! | [`CoroutineState`]                     | `CoroutineState`              | `GeneratorState`      |
//!
//! What neither of those offers on stable Rust, and the reason this module exists: the
//! body is an ordinary `async` block that awaits real I/O between yields, while every
//! yield still takes an answer. `resume` is therefore itself a future, driven by whatever
//! runtime the caller uses. `genawaiter` only resumes an async body without an argument.
//!
//! How it is built, and where it differs from `std`:
//! - `yield_` sends the value with a one-shot reply slot over a channel and awaits the
//!   reply; `resume` polls the body and that channel together and returns whichever
//!   produces first. The body is polled in place: no task is spawned
//! - The first `resume` takes `None` and starts the body; each later one answers the
//!   pending yield with `Some`. `std` passes the first argument in as the closure's
//!   parameter instead
//! - `Co` is `Clone`, so one body can have several yields pending at once, for example
//!   under `join!`. They come out one per `resume`, in the order they were made, and each
//!   answer goes back to the yield that asked for it
//! - Dropping a `resume` future before it finishes leaves the coroutine where it was;
//!   resume it again with `None`. Dropping the coroutine, or [`Coroutine::cancel`], drops
//!   the body, and any `Co` that escaped it gets [`Abandoned`] instead of waiting forever

use std::{fmt, future::Future, pin::Pin};

use tokio::sync::{mpsc, oneshot};

/// What one `resume` produced, as in [`std::ops::CoroutineState`].
#[derive(Debug, PartialEq, Eq)]
pub enum CoroutineState<Y, C> {
    Yielded(Y),
    Complete(C),
}

/// A `resume` that broke the yield/answer protocol, or a yield whose answer never came.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResumeError {
    /// Resumed after the body returned or was cancelled.
    Finished,
    /// Resumed with `None` while a yield is waiting for its answer.
    MissingAnswer,
    /// Resumed with an answer while no yield is waiting for one.
    UnexpectedAnswer,
    /// The yield being answered stopped waiting before its answer arrived.
    AnswerDropped,
}

impl fmt::Display for ResumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Finished => "coroutine resumed after it finished",
            Self::MissingAnswer => "coroutine resumed without the answer its pending yield awaits",
            Self::UnexpectedAnswer => "coroutine resumed with an answer but no yield is pending",
            Self::AnswerDropped => "the pending yield stopped waiting for its answer",
        })
    }
}

/// The coroutine a yield was sent to is gone, so no answer will come.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Abandoned;

struct Yielded<Y, R> {
    value: Y,
    reply: oneshot::Sender<R>,
}

/// The body's handle for yielding, `genawaiter`'s `Co`.
pub struct Co<Y, R> {
    yields: mpsc::UnboundedSender<Yielded<Y, R>>,
}

impl<Y, R> Clone for Co<Y, R> {
    fn clone(&self) -> Self {
        Self {
            yields: self.yields.clone(),
        }
    }
}

impl<Y, R> Co<Y, R> {
    pub async fn yield_(&self, value: Y) -> Result<R, Abandoned> {
        let (reply, answer) = oneshot::channel();
        self.yields
            .send(Yielded { value, reply })
            .map_err(|_| Abandoned)?;
        answer.await.map_err(|_| Abandoned)
    }
}

type Body<C> = Pin<Box<dyn Future<Output = C> + Send>>;

pub struct Coroutine<Y, R, C> {
    body: Option<Body<C>>,
    yields: mpsc::UnboundedReceiver<Yielded<Y, R>>,
    pending: Option<oneshot::Sender<R>>,
}

impl<Y, R, C> Coroutine<Y, R, C> {
    /// Builds the body from `producer`. Nothing runs until the first `resume`.
    pub fn new<F>(producer: impl FnOnce(Co<Y, R>) -> F) -> Self
    where
        F: Future<Output = C> + Send + 'static,
    {
        let (sender, yields) = mpsc::unbounded_channel();
        Self {
            body: Some(Box::pin(producer(Co { yields: sender }))),
            yields,
            pending: None,
        }
    }

    pub async fn resume(&mut self, answer: Option<R>) -> Result<CoroutineState<Y, C>, ResumeError> {
        let Some(body) = self.body.as_mut() else {
            return Err(ResumeError::Finished);
        };
        match (self.pending.take(), answer) {
            (None, None) => {}
            (Some(reply), Some(answer)) => {
                reply.send(answer).map_err(|_| ResumeError::AnswerDropped)?
            }
            (pending @ Some(_), None) => {
                self.pending = pending;
                return Err(ResumeError::MissingAnswer);
            }
            (None, Some(_)) => return Err(ResumeError::UnexpectedAnswer),
        }
        tokio::select! {
            biased;
            Some(Yielded { value, reply }) = self.yields.recv() => {
                self.pending = Some(reply);
                Ok(CoroutineState::Yielded(value))
            }
            output = body => {
                self.cancel();
                Ok(CoroutineState::Complete(output))
            }
        }
    }

    /// Drops the body and fails every yield still waiting, or yet to be made, with
    /// [`Abandoned`].
    pub fn cancel(&mut self) {
        self.body = None;
        self.pending = None;
        self.yields.close();
        while self.yields.try_recv().is_ok() {}
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use rstest::rstest;
    use tokio::time::timeout;

    use super::{Abandoned, Co, Coroutine, CoroutineState, ResumeError};

    type Test<C> = Coroutine<u32, &'static str, C>;

    fn yielded<C>(state: Result<CoroutineState<u32, C>, ResumeError>) -> u32 {
        match state {
            Ok(CoroutineState::Yielded(value)) => value,
            Ok(CoroutineState::Complete(_)) => panic!("expected a yield, the body returned"),
            Err(error) => panic!("expected a yield, resume failed: {error}"),
        }
    }

    fn complete<C>(state: Result<CoroutineState<u32, C>, ResumeError>) -> C {
        match state {
            Ok(CoroutineState::Complete(output)) => output,
            Ok(CoroutineState::Yielded(value)) => panic!("expected completion, got yield {value}"),
            Err(error) => panic!("expected completion, resume failed: {error}"),
        }
    }

    /// A body parked at one yield, with nothing else going on.
    fn suspended_once() -> Test<Result<&'static str, Abandoned>> {
        Coroutine::new(|co| async move { co.yield_(1).await })
    }

    #[tokio::test]
    async fn each_answer_resumes_the_yield_that_asked_for_it() {
        let mut coroutine: Test<String> = Coroutine::new(|co| async move {
            let first = co.yield_(1).await.unwrap();
            let second = co.yield_(2).await.unwrap();
            format!("{first}+{second}")
        });

        assert_eq!(yielded(coroutine.resume(None).await), 1);
        assert_eq!(yielded(coroutine.resume(Some("a")).await), 2);
        assert_eq!(complete(coroutine.resume(Some("b")).await), "a+b");
    }

    #[tokio::test]
    async fn the_body_awaits_real_futures_between_yields() {
        let mut coroutine: Test<()> = Coroutine::new(|co| async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            co.yield_(7).await.unwrap();
        });

        assert_eq!(yielded(coroutine.resume(None).await), 7);
    }

    #[tokio::test]
    async fn concurrent_yields_come_out_in_order_and_are_answered_separately() {
        let mut coroutine: Test<(&str, &str)> = Coroutine::new(|co| async move {
            let (first, second) = tokio::join!(co.yield_(1), co.yield_(2));
            (first.unwrap(), second.unwrap())
        });

        assert_eq!(yielded(coroutine.resume(None).await), 1);
        assert_eq!(yielded(coroutine.resume(Some("one")).await), 2);
        assert_eq!(
            complete(coroutine.resume(Some("two")).await),
            ("one", "two")
        );
    }

    #[rstest]
    #[case::answer_before_any_yield(&[], Some("early"), ResumeError::UnexpectedAnswer)]
    #[case::no_answer_for_a_pending_yield(&[None], None, ResumeError::MissingAnswer)]
    #[case::resumed_after_returning(&[None, Some("done")], None, ResumeError::Finished)]
    #[case::answered_after_returning(&[None, Some("done")], Some("late"), ResumeError::Finished)]
    #[tokio::test]
    async fn protocol_violations_are_reported(
        #[case] before: &[Option<&'static str>],
        #[case] answer: Option<&'static str>,
        #[case] expected: ResumeError,
    ) {
        let mut coroutine = suspended_once();
        for step in before {
            coroutine.resume(*step).await.unwrap();
        }

        assert_eq!(coroutine.resume(answer).await.unwrap_err(), expected);
    }

    #[tokio::test]
    async fn a_missing_answer_leaves_the_yield_waiting_for_the_real_one() {
        let mut coroutine = suspended_once();
        assert_eq!(yielded(coroutine.resume(None).await), 1);
        assert_eq!(
            coroutine.resume(None).await.unwrap_err(),
            ResumeError::MissingAnswer
        );

        assert_eq!(complete(coroutine.resume(Some("real")).await), Ok("real"));
    }

    #[tokio::test]
    async fn answering_a_yield_that_stopped_waiting_is_reported() {
        let mut coroutine: Test<()> = Coroutine::new(|co| async move {
            tokio::select! {
                biased;
                _ = co.yield_(1) => {}
                () = std::future::ready(()) => {}
            }
            std::future::pending::<()>().await;
        });
        assert_eq!(yielded(coroutine.resume(None).await), 1);

        assert_eq!(
            coroutine.resume(Some("late")).await.unwrap_err(),
            ResumeError::AnswerDropped
        );
    }

    #[tokio::test]
    async fn a_dropped_resume_leaves_the_coroutine_resumable() {
        let mut coroutine: Test<()> = Coroutine::new(|co| async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            co.yield_(3).await.unwrap();
        });
        assert!(
            timeout(Duration::from_millis(1), coroutine.resume(None))
                .await
                .is_err()
        );

        assert_eq!(yielded(coroutine.resume(None).await), 3);
    }

    struct Dropped(Arc<Mutex<bool>>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            *self.0.lock().unwrap() = true;
        }
    }

    #[tokio::test]
    async fn cancel_drops_the_body_and_ends_the_coroutine() {
        let dropped = Arc::new(Mutex::new(false));
        let guard = Dropped(Arc::clone(&dropped));
        let mut coroutine: Test<()> = Coroutine::new(|co| async move {
            let _guard = guard;
            co.yield_(1).await.unwrap();
        });
        yielded(coroutine.resume(None).await);

        coroutine.cancel();

        assert!(*dropped.lock().unwrap());
        assert_eq!(
            coroutine.resume(Some("after")).await.unwrap_err(),
            ResumeError::Finished
        );
    }

    #[rstest]
    #[case::cancelled(true)]
    #[case::dropped(false)]
    #[tokio::test]
    async fn a_co_that_escaped_the_body_is_abandoned_once_the_coroutine_ends(#[case] cancel: bool) {
        let escaped: Arc<Mutex<Option<Co<u32, &'static str>>>> = Arc::default();
        let slot = Arc::clone(&escaped);
        let mut coroutine: Test<()> = Coroutine::new(move |co| {
            *slot.lock().unwrap() = Some(co.clone());
            async move {
                co.yield_(1).await.unwrap();
            }
        });
        yielded(coroutine.resume(None).await);
        let co = escaped.lock().unwrap().take().unwrap();
        let waiting = tokio::spawn(async move { co.yield_(2).await });
        tokio::task::yield_now().await;

        if cancel {
            coroutine.cancel();
        } else {
            drop(coroutine);
        }

        let outcome = timeout(Duration::from_secs(1), waiting)
            .await
            .expect("an escaped yield waits forever")
            .unwrap();
        assert_eq!(outcome, Err(Abandoned));
    }
}
