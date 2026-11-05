use std::{sync::{Arc, atomic::{AtomicBool, Ordering}}, thread::JoinHandle};
use crate::prelude::*;

pub mod prelude {
    pub use libtux::prelude::Time;
    pub use super::results::prelude::*;
    pub use super::tasks::*;
    pub use super::utils::prelude::*;
    pub use super::{
        RtSynthThread,
        TerminateSignal,
        start_rt_thread,
    };
}

pub mod results;
pub mod utils;

pub mod tasks {
    pub mod periodic_task;
}

#[derive(Debug, Clone)]
pub struct TerminateSignal(Arc<AtomicBool>);

impl TerminateSignal {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn get(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    pub fn set(&mut self, value: bool) {
        self.0.store(value, Ordering::Relaxed);
    }
}

pub fn start_rt_thread<F, T>(function: F, name: &str, rt_prio: Option<i32>, signal: TerminateSignal) -> JoinHandle<anyhow::Result<T>>
    where F: FnOnce(TerminateSignal) -> anyhow::Result<T> + Send + 'static, T: Send + 'static
{
    use libtux::prelude::*;

    let policy = rt_prio
        .map(|prio| pid::SchedPolicy::FIFO(prio))
        .unwrap_or(pid::SchedPolicy::other());

    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            pid::set_sched_policy(
                Pid(std::process::id()),
                policy,
                pid::SchedFlags::empty()
            )?;

            function(signal)
        })
        .unwrap()
}

pub trait RtSynthThread {
    fn run<W: std::fmt::Write>(&self, abs_start_time: Time, max_time: Option<Time>, signal: TerminateSignal, writer: W) -> Result<(), std::fmt::Error>;
}
