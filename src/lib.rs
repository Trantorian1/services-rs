//! # Services Architecture
//!
//! This crate follows a [`microservice`] architecture inspired by the Erlang Virtual Machine in
//! order to simplify the composability and parallelism of parts of your application. That is to say
//! services can be started in different orders, at different points in the program's execution,
//! stopped and even restarted. The advantage in parallelism arises from the fact that each services
//! runs as its own non-blocking asynchronous task which allows for high throughput. Inter-service
//! communication is done via [`tokio::sync`] for status updates, with further message passing
//! being the responsability of the developer.
//!
//! Services are run to completion until no service remains, at which point the service monitor will
//! automatically shutdown.
//!
//! # The [`Service`] trait
//!
//! This is the backbone of this crate. The [`Service`] trait specifies how a service must start.
//! To be identified, a [`Service`] must also implement [`ServiceIdProvider`] (this is a separate trait
//! for reasons related to boxing). Services can be identified by any type which implements
//! [`ServiceId`].
//!
//! Under the hood, services are identified using [`String`]s, but this is wrapped around the
//! [`ServiceId`] trait to provide type safety.
//!
//! Services are started from [`Service::start`] using [`ServiceRunner::service_loop`]. [`service_loop`]
//! is a function which takes in a future: this is the main loop of a service, and should run until
//! the service completes or is [`cancelled`].
//!
//! It is part of the contract of the [`Service`] trait that calls to [`service_loop`] **should not
//! complete** until the service has _finished_ execution as this is used to mark a service as
//! ready to restart. This means your service should keep yielding [`Poll::Pending`] until it is done.
//! Services where [`service_loop`] completes _before_ the service has finished execution will be
//! automatically marked for shutdown. This is done to avoid an invalid state. This means you should
//! not run the work your service does inside a [`tokio::task::spawn`] and exit [`service_loop`]
//! immediately for example. For running blocking futures inside your service, refer to
//! [`ServiceContext::run_until_cancelled`].
//!
//! It is assumed that services can and might be restarted. You have the responsibility to ensure
//! this is possible. This means you should make sure not to use the likes of [`std::mem::take`] or
//! similar inside [`Service::start`]. In general, make sure your service still contains all the
//! necessary information it needs to restart. This might mean certain attributes need to be
//! stored as a [`std::sync::Arc`] and cloned so that the future in [`service_loop`] can safely take
//! ownership of them.
//!
//! ## An incorrect implementation of the [`Service`] trait
//!
//! ```rust
//! # use service_rs::Service;
//! # use service_rs::ServiceId;
//! # use service_rs::ServiceIdProvider;
//! # use service_rs::ServiceRunner;
//! pub struct MyService;
//!
//! #[async_trait::async_trait]
//! impl Service for MyService {
//!     async fn start<'a>(&mut self, runner: ServiceRunner<'a>) -> anyhow::Result<()> {
//!         runner.service_loop(move |ctx| async {
//!             tokio::task::spawn(async {
//!                 tokio::time::sleep(std::time::Duration::MAX).await;
//!             });
//!
//!             // This is incorrect, as the future passed to service_loop will
//!             // resolve before the task spawned above completes, meaning the
//!             // service monitor will incorrectly mark this service as ready
//!             // to restart. In a more complex scenario, this means we might
//!             // enter an invalid state!
//!             anyhow::Ok(())
//!         })
//!     }
//! }
//!
//! impl ServiceIdProvider for MyService {
//!     fn id_provider(&self) -> impl ServiceId {
//!         MyServiceId
//!     }
//! }
//!
//! pub struct MyServiceId;
//!
//! impl ServiceId for MyServiceId {
//!     fn svc_id(&self) -> String {
//!         "MyService".to_string()
//!     }
//! }
//! ```
//!
//! ## A correct implementation of the [`Service`] trait
//!
//! ```rust
//! # use service_rs::Service;
//! # use service_rs::ServiceId;
//! # use service_rs::ServiceIdProvider;
//! # use service_rs::ServiceRunner;
//! pub struct MyService;
//!
//! #[async_trait::async_trait]
//! impl Service for MyService {
//!     async fn start<'a>(&mut self, runner: ServiceRunner<'a>) -> anyhow::Result<()> {
//!         runner.service_loop(move |ctx| async move {
//!             ctx.run_until_cancelled(tokio::time::sleep(std::time::Duration::MAX)).await;
//!
//!             // This is correct, as the future passed to service_loop will
//!             // only resolve once the task above completes, so the service
//!             // monitor can correctly mark this service as ready to restart.
//!             anyhow::Ok(())
//!         })
//!     }
//! }
//!
//! impl ServiceIdProvider for MyService {
//!     fn id_provider(&self) -> impl ServiceId {
//!         MyServiceId
//!     }
//! }
//!
//! pub struct MyServiceId;
//!
//! impl ServiceId for MyServiceId {
//!     fn svc_id(&self) -> String {
//!         "MyService".to_string()
//!     }
//! }
//! ```
//!
//! Or if you really need to spawn a background task:
//!
//! ```rust
//! # use service_rs::Service;
//! # use service_rs::ServiceId;
//! # use service_rs::ServiceIdProvider;
//! # use service_rs::ServiceRunner;
//! pub struct MyService;
//!
//! #[async_trait::async_trait]
//! impl Service for MyService {
//!     async fn start<'a>(&mut self, runner: ServiceRunner<'a>) -> anyhow::Result<()> {
//!         runner.service_loop(move |ctx| async move {
//!             let ctx1 = ctx.clone();
//!             tokio::task::spawn(async move {
//!                 ctx1.run_until_cancelled(tokio::time::sleep(std::time::Duration::MAX)).await;
//!             });
//!
//!             ctx.cancelled().await;
//!
//!             // This is correct, as even though we are spawning a background
//!             // task we have implemented a cancellation mechanism with the
//!             // service context and are waiting for that cancellation in the
//!             // service loop.
//!             anyhow::Ok(())
//!         })
//!     }
//! }
//!
//! impl ServiceIdProvider for MyService {
//!     fn id_provider(&self) -> impl ServiceId {
//!         MyServiceId
//!     }
//! }
//!
//! pub struct MyServiceId;
//!
//! impl ServiceId for MyServiceId {
//!     fn svc_id(&self) -> String {
//!         "MyService".to_string()
//!     }
//! }
//! ```
//!
//! This sort of problem generally arises in cases where the service's role is to spawn another
//! background task (when starting a server for example). Either avoid spawning a detached task or
//! use mechanisms such as [`cancelled`] to await for the service's completion.
//!
//! Note that service shutdown is designed to be manual, ie: services should never be forcefully
//! shutdown unless you forget to implement proper [`cancellation`]. To avoid this edge case, we still
//! implement a [`SERVICE_GRACE_PERIOD`] which is the maximum duration a service is allowed to take to
//! shutdown, after which it is forcefully cancelled. This should not happen in practice and only
//! serves to avoid cases where someone would forget to implement a cancellation check.
//!
//! ---
//!
//! # Cancellation status and inter-process requests
//!
//! Services are passed a [`ServiceContext`] as part of [`service_loop`] to be used during their
//! execution to check for and request cancellation. Services can also start child services with
//! [`ServiceContext::child`] to create a hierarchy of services.
//!
//! ## Cancellation checks
//!
//! [`ServiceContext`] allows you to gracefully handle the shutting down services by manually checking
//! for  cancellation at logical points in the execution. You can use the following methods to check
//! for cancellation.
//!
//! - [`is_cancelled`]: synchronous, useful in non-blocking scenarios.
//! - [`cancelled`]: a future which resolves upon service cancellation. Useful to wait on a service or
//!   alongside [`tokio::select`].
//!
//! <div class="warning">
//!
//! It is your responsibility to check for cancellation inside of your service. If you do not, or
//! your service takes longer than [`SERVICE_GRACE_PERIOD`] to shutdown, then your service will be
//! forcefully cancelled.
//!
//! </div>
//!
//! ## Cancellation requests
//!
//! Any service with access to a [`ServiceContext`] can request the cancellation of _any other
//! service, at any point during execution_. This can be used for error handling for example, by
//! having a single service shut itself down without affecting other services, or for administrative
//! and testing purposes by toggling services on and off.
//!
//! You can use the following methods to request for the cancellation of a
//! service:
//!
//! - [`cancel_global`]: cancels all services.
//! - [`cancel_local`]: cancels this service and all its children.
//! - [`service_deactivate`]: cancel a specific service.
//!
//! ## Start requests
//!
//! You can _request_ for a service to be started by calling [`service_activate`]. Note that this will only
//! work if the service has already been registered at the start of the program.
//!
//! # Service orchestration
//!
//! Services are orchestrated by a [`ServiceMonitor`], which is responsible for registering services,
//! marking them as active or inactive as well as starting and restarting them upon request.
//! [`ServiceMonitor`] also handles the cancellation of all services upon receiving a `SIGINT` or
//! `SIGTERM`.
//!
//! <div class="warning">
//!
//! Services cannot be started or restarted if they have not been registered at startup.
//!
//! </div>
//!
//! ## example:
//!
//! ```rust
//! # use service_rs::Service;
//! # use service_rs::ServiceId;
//! # use service_rs::ServiceIdProvider;
//! # use service_rs::ServiceRunner;
//! # use service_rs::ServiceMonitorBuilder;
//! #
//! # pub struct MyService;
//! #
//! # #[async_trait::async_trait]
//! # impl Service for MyService {
//! #     async fn start<'a>(&mut self, runner: ServiceRunner<'a>) -> anyhow::Result<()> {
//! #         runner.service_loop(move |ctx| async move {
//! #             anyhow::Ok(())
//! #         })
//! #     }
//! # }
//! #
//! # impl ServiceIdProvider for MyService {
//! #     fn id_provider(&self) -> impl ServiceId {
//! #         MyServiceId
//! #     }
//! # }
//! #
//! # pub struct MyServiceId;
//! #
//! # impl ServiceId for MyServiceId {
//! #     fn svc_id(&self) -> String {
//! #         "MyService".to_string()
//! #     }
//! # }
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     ServiceMonitorBuilder::new()
//!         .await
//!         .with(MyService)?
//!         .activate(MyServiceId)
//!         .await?
//!         .start()
//!         .await
//! }
//! ```
//!
//! # Composing services
//!
//! Services can run other services, allowing you to create a [hierarchy] of services. Services
//! which manage other services are known as parent services, and services which are managed by
//! other services are known as [`child`] services. A service can be both a child and parent
//! service.If a parent service is cancelled, or stopped, then all of its child services will be
//! stopped as well.
//!
//! ## example:
//!
//! ```rust
//! # use service_rs::Service;
//! # use service_rs::ServiceId;
//! # use service_rs::ServiceIdProvider;
//! # use service_rs::ServiceRunner;
//! # use service_rs::ServiceMonitorBuilder;
//! struct ServiceA;
//! struct ServiceB;
//! struct ServiceC;
//!
//! enum MyServiceId {
//!     ServiceA,
//!     ServiceB,
//!     ServiceC,
//! }
//!
//! impl ServiceId for MyServiceId {
//!     fn svc_id(&self) -> String {
//!         match self {
//!             Self::ServiceA => "ServiceA".to_string(),
//!             Self::ServiceB => "ServiceB".to_string(),
//!             Self::ServiceC => "ServiceC".to_string(),
//!         }
//!     }
//! }
//!
//! #[async_trait::async_trait]
//! impl Service for ServiceA {
//!     async fn start<'a>(&mut self, runner: ServiceRunner<'a>) -> anyhow::Result<()> {
//!         runner.service_loop(move |ctx| async move {
//!             // Service A is the parent of services B and C. If A shuts down,
//!             // so will B and C.
//!             ctx.child()
//!                 .await
//!                 .with_active(ServiceB)
//!                 .await?
//!                 .with_active(ServiceC)
//!                 .await?
//!                 .start()
//!                 .await
//!         })
//!     }
//! }
//!
//! #[async_trait::async_trait]
//! impl Service for ServiceB {
//!     async fn start<'a>(&mut self, runner: ServiceRunner<'a>) -> anyhow::Result<()> {
//!         runner.service_loop(move |ctx| async move {
//!             ctx.cancelled().await;
//!             anyhow::Ok(())
//!         })
//!     }
//! }
//!
//! #[async_trait::async_trait]
//! impl Service for ServiceC {
//!     async fn start<'a>(&mut self, runner: ServiceRunner<'a>) -> anyhow::Result<()> {
//!         runner.service_loop(move |ctx| async move {
//!             ctx.cancelled().await;
//!             anyhow::Ok(())
//!         })
//!     }
//! }
//!
//! impl ServiceIdProvider for ServiceA {
//!     fn id_provider(&self) -> impl ServiceId {
//!         MyServiceId::ServiceA
//!     }
//! }
//!
//! impl ServiceIdProvider for ServiceB {
//!     fn id_provider(&self) -> impl ServiceId {
//!         MyServiceId::ServiceB
//!     }
//! }
//!
//! impl ServiceIdProvider for ServiceC {
//!     fn id_provider(&self) -> impl ServiceId {
//!         MyServiceId::ServiceC
//!     }
//! }
//! ```
//!
//! [`microservice`]: https://en.wikipedia.org/wiki/Microservices
//! [`service_loop`]: ServiceRunner::service_loop
//! [`cancelled`]: ServiceContext::run_until_cancelled
//! [`cancellation`]: ServiceContext::run_until_cancelled
//! [`is_cancelled`]: ServiceContext::is_cancelled
//! [`Poll::Pending`]: std::task::Poll::Pending
//! [`cancel_global`]: ServiceContext::cancel_global
//! [`cancel_local`]: ServiceContext::cancel_local
//! [`service_deactivate`]: ServiceContext::service_deactivate
//! [`service_activate`]: ServiceContext::service_activate
//! [hierarchy]: ServiceContext#scope
//! [`child`]: ServiceContext::child

mod service;

pub use service::*;

#[repr(transparent)]
struct Frozen<T>(T);

impl<T> std::ops::Deref for Frozen<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Default> Default for Frozen<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T> Frozen<T> {
    fn into_inner(self) -> T {
        self.0
    }
}

trait Freeze<T>
where
    Self: Sized,
{
    fn freeze(self) -> Frozen<T>;
}

impl<T: Sized> Freeze<T> for T {
    fn freeze(self) -> Frozen<T> {
        Frozen(self)
    }
}
