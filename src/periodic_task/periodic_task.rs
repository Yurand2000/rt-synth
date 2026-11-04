use crate::prelude::*;
use libtux::prelude::*;

#[derive(Debug, Clone)]
pub struct Task {
    pub name: String,
    pub runtime: Time,
    pub deadline: Time,
    pub period: Time,
    pub calibration_value: Option<Time>,
}

impl RtSynthThread for Task {
    fn run<W: std::fmt::Write>(&self, abs_start_time: Time, max_time: Option<Time>, signal: TerminateSignal, writer: W) {
        let cpu_hogger = new_cpu_hogger(self.runtime, self.calibration_value);
        // let mut results = ResultWriter::new(&self.name, self.runtime);

        let start_time = abs_start_time;
        let mut next_activation = start_time;
        let mut end_time = start_time;
        clock::Monotonic::sleep_absolute(start_time);
        while !signal.get() && max_time.map(|max| max > (end_time - start_time)).unwrap_or(true) {
            // perform current job
            let job_start_time = next_activation;
            cpu_hogger.hog();
            let job_end_time = clock::Monotonic::now();

            // report results
            // results.push((
            //     next_activation,
            //     next_activation,
            //     job_start_time,
            //     next_activation + self.deadline,
            //     job_end_time,
            // ));

            // wait for next job
            next_activation = next_activation + self.period;
            clock::Monotonic::sleep_absolute(next_activation);

            end_time = clock::Monotonic::now();
        }

        // report non-performed jobs
        // let total_jobs = (max_time.unwrap_or(end_time - start_time) / self.period).floor() as usize;
        // let performed_jobs = results.len();

        // for job_num in performed_jobs..total_jobs {
        //     let next_activation = job_num as f64 * self.period + start_time;

        //     results.push((
        //         next_activation,
        //         Time::nanos(f64::MAX),
        //         Time::nanos(f64::MAX),
        //         (next_activation + self.deadline),
        //         Time::nanos(f64::MAX),
        //     ));
        // }

        // results.write_back(writer);
    }
}
