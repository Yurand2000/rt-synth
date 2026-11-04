use crate::prelude::*;
use libtux::prelude::*;

pub mod prelude {
    pub use super::{
        CpuHogger,
        new_cpu_hogger,
        calibrate,
        cpuhog_for,
        cpuhog_until,
        cpuhog_loop,
    };
}

pub trait CpuHogger {
    fn hog(&self);
}

pub struct CpuHoggerRelativeTime {
    time: Time,
}

pub struct CpuHoggerIterations {
    iterations: usize,
}

impl CpuHoggerRelativeTime {
    pub fn new(time: Time) -> Self {
        Self { time }
    }
}

impl CpuHogger for CpuHoggerRelativeTime {
    fn hog(&self) {
        cpuhog_for(self.time, 500);
    }
}

impl CpuHoggerIterations {
    pub fn new(time: Time, calibration: Time) -> Self {
        Self { iterations: (time / calibration).floor() as usize }
    }
}

impl CpuHogger for CpuHoggerIterations {
    fn hog(&self) {
        cpuhog_loop(self.iterations);
    }
}

pub fn new_cpu_hogger(time: Time, calibration_value: Option<Time>) -> Box<dyn CpuHogger> {
    if let Some(calibration) = calibration_value {
        Box::new(CpuHoggerIterations::new(time, calibration))
    } else {
        Box::new(CpuHoggerRelativeTime::new(time))
    }
}

pub fn calibrate(iterations: usize) -> Time {
    let start_time = std::time::Instant::now();

    cpuhog_loop(iterations);

    let end_time = std::time::Instant::now();
    let elapsed_time = end_time - start_time;

    Time::secs(elapsed_time.as_secs_f64()) / iterations as f64
}

pub fn cpuhog_loop(iterations: usize) {
    let mut sum = 0;
    for i in 1 ..= iterations {
        for j in 1 ..= 100 {
            sum = sum + sum % (i + j);
        }
    }
}

pub fn cpuhog_for(rel_time: Time, cycle_iterations: usize) {
    let abs_time = clock::Monotonic::now() + rel_time;
    cpuhog_until(abs_time, cycle_iterations);
}

pub fn cpuhog_until(abs_time: Time, cycle_iterations: usize) {
    while clock::Monotonic::now() < abs_time {
        cpuhog_loop(cycle_iterations);
    }
}
