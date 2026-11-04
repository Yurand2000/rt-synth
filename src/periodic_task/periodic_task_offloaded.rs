use std::{
    io::{Read, Write}, net::*, ops::DerefMut, os::fd::AsRawFd, sync::{Arc, Condvar, Mutex}
};

use crate::prelude::*;
use libtux::prelude::*;

pub mod prelude {
    pub use super::{
        Task as PeriodicTaskOffloaded,
        PTOClient,
        PTOServerProducer,
        PTOServerConsumer,
        PTOResultWriter,
    };
}

#[derive(Debug, Clone)]
pub struct Task {
    pub name: String,
    pub uplink: Time,
    pub runtime: Time,
    pub downlink: Time,
    pub deadline: Time,
    pub period: Time,
    pub calibration_value: Option<Time>,
}

#[derive(Debug, Clone)]
struct OffloadMessage {
    job_id: u32,
    client_no: u32,
    uplink_time: Time,
    runtime: Time,
}

impl OffloadMessage {
    pub const DATA_SIZE: usize = 24;

    pub fn to_bytes(&self, data: &mut [u8]) -> anyhow::Result<()> {
        if data.len() < Self::DATA_SIZE {
            anyhow::bail!("")
        } else {
            data[0..4].copy_from_slice(&self.job_id.to_be_bytes());
            data[4..8].copy_from_slice(&self.client_no.to_be_bytes());
            data[8..16].copy_from_slice(&self.runtime.as_nanos().to_be_bytes());
            data[16..24].copy_from_slice(&self.uplink_time.as_nanos().to_be_bytes());

            Ok(())
        }
    }

    pub fn from_bytes(data: &[u8]) -> anyhow::Result<Self> {
        if data.len() < Self::DATA_SIZE {
            anyhow::bail!("")
        } else {
            let mut job_id = [0; 4];
            job_id.copy_from_slice(&data[0..4]);
            let mut client_no = [0; 4];
            client_no.copy_from_slice(&data[4..8]);
            let mut runtime = [0; 8];
            runtime.copy_from_slice(&data[8..16]);
            let mut uplink_time = [0; 8];
            uplink_time.copy_from_slice(&data[16..24]);

            Ok(Self {
                job_id: u32::from_be_bytes(job_id),
                client_no: u32::from_be_bytes(client_no),
                runtime: Time::nanos(f64::from_be_bytes(runtime)),
                uplink_time: Time::nanos(f64::from_be_bytes(uplink_time)),
            })
        }
    }
}

pub struct PTOClient {
    task: Task,
    client_no: u32,
    address: SocketAddr,
}

impl PTOClient {
    pub fn new(task: Task, client_no: u32, address: SocketAddr) -> Self {
        Self {
            task,
            client_no,
            address,
        }
    }

    pub fn name(&self) -> &str {
        &self.task.name
    }
}

impl RtSynthThread for PTOClient {
    fn run<W: std::fmt::Write>(&self, abs_start_time: Time, max_time: Option<Time>, signal: TerminateSignal, writer: W) {
        let mut results = PTOResultWriter::new(PTOTaskHeader { task: &self.task });

        let job_offload = move |job_id, abs_deadline| -> anyhow::Result<OffloadMessage> {
            let msg_data = OffloadMessage {
                job_id,
                client_no: self.client_no,
                runtime: self.task.runtime,
                uplink_time: self.task.uplink,
            };
            let next_timeout = || -> std::time::Duration {
                let timeout: Time = abs_deadline - clock::Monotonic::now();
                std::time::Duration::from_nanos(timeout.as_nanos() as u64)
            };

            let mut buffer = [0; OffloadMessage::DATA_SIZE];
            msg_data.to_bytes(&mut buffer)?;
            let mut stream = TcpStream::connect_timeout(&self.address, next_timeout())?;
            // let mut stream = TcpStream::connect(&self.address)?;
            // stream.set_write_timeout(Some(next_timeout()))?;
            stream.write(&buffer)?;
            // stream.set_read_timeout(Some(next_timeout()))?;
            stream.read(&mut buffer)?;

            let out_data = OffloadMessage::from_bytes(&buffer)?;

            if out_data.client_no == msg_data.client_no && out_data.job_id == msg_data.job_id {
                Ok(out_data)
            } else {
                anyhow::bail!("Offload processing error")
            }
        };

        let start_time = abs_start_time;
        let mut next_activation = start_time;
        let mut end_time = start_time;
        let mut job_id = 0;
        clock::Monotonic::sleep_absolute(start_time);
        while !signal.get() && max_time.map(|max| max > (end_time - start_time)).unwrap_or(true) {
            // perform current job
            let job_abs_deadline = next_activation + self.task.deadline;
            let job_start_time = clock::Monotonic::now();
            let (job_uplink_finish, job_downlink_start) =
                match job_offload(job_id, job_abs_deadline) {
                    Ok(job_data) => {
                        (
                            job_start_time + job_data.uplink_time,
                            clock::Monotonic::now(),
                        )
                    },
                    Err(e) => {
                        eprintln!("Job Offload error: {e}");
                        (
                            Time::nanos(u64::MAX as f64),
                            Time::nanos(u64::MAX as f64),
                        )
                    }
                };

            // report results
            results.push(PTOJobHeader {
                release: next_activation,
                start: job_start_time,
                uplink_finish: job_uplink_finish,
                downlink_start: job_downlink_start,
                end: job_downlink_start + self.task.downlink,
            });

            // wait for next job
            next_activation = next_activation + self.task.period;
            clock::Monotonic::sleep_absolute(next_activation);

            end_time = clock::Monotonic::now();
            job_id += 1;
        }

        // report non-performed jobs
        let total_jobs = (max_time.unwrap_or(end_time - start_time) / self.task.period).floor() as usize;
        let performed_jobs = results.len();

        for job_num in performed_jobs..total_jobs {
            let next_activation = job_num as f64 * self.task.period + start_time;

            results.push(PTOJobHeader {
                release: next_activation,
                start: Time::nanos(u64::MAX as f64),
                uplink_finish: Time::nanos(u64::MAX as f64),
                downlink_start: Time::nanos(u64::MAX as f64),
                end: Time::nanos(u64::MAX as f64),
            });
        }

        results.write_back(writer);
    }
}

struct InMessage {
    job_id: u32,
    arrival_time: Time,
    uplink_time: Time,
    runtime: Time,
    stream: TcpStream,
}

pub struct PTOServerProducer {
    address: SocketAddr,
    in_queues: Vec<Arc<(Mutex<Option<InMessage>>, Condvar)>>,
}

pub struct PTOServerConsumer {
    client_no: u32,
    in_queue: Arc<(Mutex<Option<InMessage>>, Condvar)>,
    calibration_value: Option<Time>,
}

impl PTOServerProducer {
    pub fn new(num_consumers: usize, address: SocketAddr, calibration_value: Option<Time>) -> (Self, Vec<PTOServerConsumer>) {
        let queues: Vec<_> = (0 .. num_consumers)
            .map(|_| Arc::new((Mutex::new(None), Condvar::new())))
            .collect();

        let producer = Self {
            address,
            in_queues: queues.clone(),
        };

        let consumers: Vec<_> = queues.into_iter().enumerate()
            .map(|(client_no, in_queue)| {
                PTOServerConsumer {
                    client_no: client_no as u32,
                    in_queue,
                    calibration_value,
                }
            })
            .collect();

        (producer, consumers)
    }

    pub fn run(&self, abs_start_time: Time, max_time: Option<Time>, signal: TerminateSignal) {
        let socket = TcpListener::bind(self.address).unwrap();

        let mut buf = [0; OffloadMessage::DATA_SIZE];
        let mut socket_get_msg = || -> anyhow::Result<()> {
            use std::io::ErrorKind::WouldBlock;

            match socket.accept() {
                Ok((mut stream, _)) => {
                    let arrival_time = clock::Monotonic::now();

                    stream.read_exact(&mut buf)?;
                    let job_data = OffloadMessage::from_bytes(&buf)?;

                    let in_queue = self.in_queues.get(job_data.client_no as usize)
                        .ok_or(anyhow::format_err!(""))?;

                    let (in_queue, in_condvar) = &**in_queue;
                    let mut in_data = in_queue.lock().unwrap();
                    // assert!(in_data.is_none());
                    *in_data = Some(InMessage {
                        job_id: job_data.job_id,
                        arrival_time,
                        uplink_time: job_data.uplink_time,
                        runtime: job_data.runtime,
                        stream,
                    });
                    in_condvar.notify_all();

                    Ok(())
                },
                Err(err) if err.kind() == WouldBlock => Ok(()),
                Err(err) => Err(err.into()),
            }
        };

        let start_time = abs_start_time;
        let mut end_time = start_time;

        // pselect call utils
        let socket_fd = socket.as_raw_fd();
        let timespec: libc::timespec = Time::secs(1.0).into();
        let read_fd_set = (&mut [0; size_of::<libc::fd_set>()]).as_mut_ptr() as *mut libc::fd_set;

        clock::Monotonic::sleep_absolute(start_time);
        while !signal.get() && max_time.map(|max| max > (end_time - start_time)).unwrap_or(true) {
            // pselect or timeout
            let selected = unsafe {
                libc::FD_ZERO(read_fd_set);
                libc::FD_SET(socket_fd, read_fd_set);

                libc::pselect(
                    socket_fd + 1,
                    read_fd_set,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &timespec,
                    std::ptr::null()
                );

                libc::FD_ISSET(socket_fd, read_fd_set)
            };

            if selected {
                let Ok(_) = socket_get_msg() else { break; };
            }

            end_time = clock::Monotonic::now();
        }

        for in_queue in &self.in_queues {
            in_queue.1.notify_all();
        }
    }
}

impl PTOServerConsumer {
    pub fn run(&self, abs_start_time: Time, max_time: Option<Time>, signal: TerminateSignal) {
        let mut buf = [0; OffloadMessage::DATA_SIZE];
        let mut write_job = |mut job: InMessage| -> anyhow::Result<()> {
            let msg = OffloadMessage {
                job_id: job.job_id,
                client_no: self.client_no,
                uplink_time: job.uplink_time,
                runtime: job.runtime,
            };

            msg.to_bytes(&mut buf)?;
            job.stream.write(&buf)?;

            Ok(())
        };

        let start_time = abs_start_time;
        let mut end_time = start_time;

        clock::Monotonic::sleep_absolute(start_time);
        while !signal.get() && max_time.map(|max| max > (end_time - start_time)).unwrap_or(true) {
            // wait for next job
            let mut job =
            {
                let (in_queue, in_condvar) = &*self.in_queue;
                let mut guard = in_queue.lock().unwrap();
                while guard.is_none() {
                    guard = in_condvar.wait(guard).unwrap();

                    if signal.get() {
                        return;
                    }
                }

                guard.deref_mut().take().unwrap()
            };

            // perform current job
            let cpu_hogger = new_cpu_hogger(job.runtime, self.calibration_value);
            let job_recv_time = job.arrival_time + job.uplink_time;

            clock::Monotonic::sleep_absolute(job_recv_time);
            let abs_uplink_time = clock::Monotonic::now();
            job.uplink_time = abs_uplink_time - job.arrival_time;
            cpu_hogger.hog();
            job.runtime = clock::Monotonic::now() - abs_uplink_time;

            // send results back
            let Ok(_) = write_job(job) else { break; };

            end_time = clock::Monotonic::now();
        }
    }
}

pub struct PTOTaskHeader<'a> {
    task: &'a PeriodicTaskOffloaded
}

impl<'a> TaskResultData<5> for PTOTaskHeader<'a> {
    fn header() -> [&'static str; 5] {
        ["Uplink", "Runtime", "Downlink", "Deadline", "Period"]
    }

    fn name(&self) -> &str {
        &self.task.name
    }

    fn write_data<W: std::fmt::Write>(&self, mut writer: W) {
        let _ = write!(writer, "\t{:.0}\t{:.0}\t{:.0}\t{:.0}\t{:.0}",
            self.task.uplink.as_nanos(),
            self.task.runtime.as_nanos(),
            self.task.downlink.as_nanos(),
            self.task.deadline.as_nanos(),
            self.task.period.as_nanos(),
        );
    }
}

pub struct PTOJobHeader {
    release: Time,
    start: Time,
    uplink_finish: Time,
    downlink_start: Time,
    end: Time,
}

impl JobResultData<6, PTOTaskHeader<'_>> for PTOJobHeader {
    fn header() -> [&'static str; 6] {
        ["AbsReleaseTime", "AbsWakeupTime", "AbsUplinlStartTime", "AbsDownlinkStartTime", "AbsEndTime", "AbsDeadline"]
    }

    fn write_data<W: std::fmt::Write>(&self, task: &PTOTaskHeader, mut writer: W) {
        let _ = write!(writer, "\t{:.0}\t{:.0}\t{:.0}\t{:.0}\t{:.0}\t{:.0}",
            self.release.as_nanos(),
            self.start.as_nanos(),
            self.uplink_finish.as_nanos(),
            self.downlink_start.as_nanos(),
            self.end.as_nanos(),
            (self.release + task.task.deadline).as_nanos(),
        );
    }
}

pub type PTOResultWriter<'a> = ResultWriter<5, 6, PTOTaskHeader<'a>, PTOJobHeader>;
