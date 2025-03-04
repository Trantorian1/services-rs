# `Service-rs`

_A composable service library for Rust, inspired by the Erlang virual machine._

## General information

`service-rs` is a Rust library which allows you to easily structure your application as composable, inter-dependant services with strong error handling and the ability to start, restart and shutdown services at runtime. It is ispired by the Erlang Virual Machine, but only handles message passing for status updates between services. 

> [!NOTE]
> Why? Because message handling is magic and makes it confusing to follow the flow of data in your application. `service-rs` allows you to compose your application as you want: single source of mutability? No problem. Want to be able to sende data between threads? Manually creates a `tokio::sync` channel or the likes. _You remain in control of data flow in your code._

### example
```rust
/// Services are identified using the `ServiceId` trait, which provides a type-safe wrapper
/// around the strings used to identify services. This means you can create as many services
/// as you want and still interact with them in a type-safe way!
pub enum MyServiceId {
    ServiceA,
    ServiceB,
    ServiceC,
}

impl ServiceId for MyServiceId {
    fn svc_id(&self) -> String {
        match self {
            Self::ServiceA => "ServiceA".to_string(),
            Self::ServiceB => "ServiceB".to_string(),
            Self::ServiceC => "ServiceC".to_string()
        }
    }
}

pub struct ServiceA;
pub struct ServiceB;
pub struct ServiceC;

#[async_trait::async_trait]
impl Service for ServiceA {
    async fn start<'a>(&mut self, runner: ServiceRunner<'a>) -> anyhow::Result<()> {
        runner.service_loop(move |ctx| async move {
            // `service-rs` makes it easy to express inter-service dependencies in a declarative way
            ctx.wait_for_running(MyServiceId::ServiceB).await;

            ctx.run_until_cancelled(async {
                // Your looping service logic here!
            }).await;
        })
    }
}

impl ServiceIdProvider for ServiceA {
    fn id_provider(&self) -> impl ServiceId {
        MyServiceId::ServiceA
    }
}

#[async_trait::async_trait]
impl Service for ServiceB {
    async fn start<'a>(&mut self, runner: ServiceRunner<'a>) -> anyhow::Result<()> {
        // `service-rs` also makes it simple to compose services together int child-parent relations!
        runner.service_loop(move |ctx| async move {
            ctx.child()
                .await
                .with_active(ServiceC)
                .start
                .await
        })
    }
}

impl ServiceIdProvider for ServiceB {
    fn id_provider(&self) -> impl ServiceId {
        MyServiceId::ServiceB
    }
}

#[async_trait::async_trait]
impl Service for ServiceC {
    async fn start<'a>(&mut self, runner: ServiceRunner<'a>) -> anyhow::Result<()> {
        runner.service_loop(move |ctx| async move {
            ctx.run_until_cancelled(async {
                // Your looping service logic here!
            }).await;
        })
    }
}

impl ServiceIdProvider for ServiceC {
    fn id_provider(&self) -> impl ServiceId {
        MyServiceId::ServiceC
    }
}
```
