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
