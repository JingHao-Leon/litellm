//! The one machine every route runs on: the route's provider future as a
//! [`Coroutine`](crate::coroutine::Coroutine) that yields [`HostOp`]s and is resumed with
//! their [`HostResult`]s. No task is spawned; dropping the machine drops the in-flight call.

use std::{future::Future, pin::Pin};

use super::{HostFailure, Interrupted, Machine, MachineStep, Step};
use crate::{
    coroutine::{Co, Coroutine, CoroutineState, ResumeError},
    event::{MachineEvent, RequestContext, WireRequest},
    host::{Demand, HostOp, HostResult},
    route::Route,
};

/// The machine's own failures, distinct from anything the provider call reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MachineFault {
    /// The host driver went away while the call was waiting on it.
    Abandoned,
    /// The host answered out of turn.
    Protocol(ResumeError),
    /// The host answered a route operation with the wrong result variant.
    Mismatch,
}

pub type ExecuteFuture<R> =
    Pin<Box<dyn Future<Output = Result<<R as Route>::Response, <R as Route>::Error>> + Send>>;

/// The provider side of the machine: how the in-flight call reaches its host.
pub struct HostChannel<R: Route> {
    co: Co<HostOp<R>, HostResult<R>>,
}

impl<R: Route> Clone for HostChannel<R> {
    fn clone(&self) -> Self {
        Self {
            co: self.co.clone(),
        }
    }
}

impl<R: Route> HostChannel<R>
where
    R::Error: From<MachineFault>,
{
    async fn invoke(&self, op: HostOp<R>) -> Result<HostResult<R>, R::Error> {
        self.co
            .yield_(op)
            .await
            .map_err(|_| MachineFault::Abandoned.into())
    }

    pub async fn route(&self, op: R::Op) -> Result<R::OpResult, R::Error> {
        match self.invoke(HostOp::Route(op)).await? {
            HostResult::Route(result) => Ok(result),
            _ => Err(MachineFault::Mismatch.into()),
        }
    }

    pub async fn before_send(
        &self,
        wire: WireRequest,
        context: RequestContext,
    ) -> Result<WireRequest, R::Error> {
        let op = HostOp::BeforeSend {
            wire: Box::new(wire),
            context: Box::new(context),
        };
        match self.invoke(op).await? {
            HostResult::BeforeSend(wire) => Ok(*wire),
            _ => Err(MachineFault::Mismatch.into()),
        }
    }

    pub async fn emit(&self, event: MachineEvent) -> Result<(), R::Error> {
        match self.invoke(HostOp::Emit(event)).await? {
            HostResult::Emitted => Ok(()),
            _ => Err(MachineFault::Mismatch.into()),
        }
    }

    pub async fn open(&self, head: R::StreamHead) -> Result<Demand, R::Error> {
        self.demand(HostOp::Open(head)).await
    }

    pub async fn deliver(&self, chunk: R::Chunk) -> Result<Demand, R::Error> {
        self.demand(HostOp::Deliver(chunk)).await
    }

    async fn demand(&self, op: HostOp<R>) -> Result<Demand, R::Error> {
        match self.invoke(op).await? {
            HostResult::Demand(demand) => Ok(demand),
            _ => Err(MachineFault::Mismatch.into()),
        }
    }
}

type RouteCoroutine<R> =
    Coroutine<HostOp<R>, HostResult<R>, Result<<R as Route>::Response, <R as Route>::Error>>;

pub struct RouteMachine<R: Route> {
    coroutine: RouteCoroutine<R>,
}

impl<R: Route> RouteMachine<R>
where
    R::Error: From<MachineFault>,
{
    pub fn new(execute: impl FnOnce(HostChannel<R>) -> ExecuteFuture<R> + Send + 'static) -> Self {
        Self {
            coroutine: Coroutine::new(|co| execute(HostChannel { co })),
        }
    }
}

impl<R: Route> Machine for RouteMachine<R>
where
    R::Error: From<MachineFault>,
{
    type Route = R;
    type Complete = R::Response;

    fn resume(&mut self, result: Option<HostResult<R>>) -> Step<'_, Self> {
        Box::pin(async move {
            match self
                .coroutine
                .resume(result)
                .await
                .map_err(MachineFault::Protocol)?
            {
                CoroutineState::Yielded(op) => Ok(MachineStep::Host(op)),
                CoroutineState::Complete(outcome) => outcome.map(MachineStep::Complete),
            }
        })
    }

    fn interrupt(&mut self, failure: HostFailure<R::Error>) -> Interrupted<'_, Self> {
        self.coroutine.cancel();
        Box::pin(async move { Err(failure.into_error()) })
    }
}
