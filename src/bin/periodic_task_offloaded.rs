use rt_synth::prelude::*;
use libtux::prelude::*;

#[derive(clap::Parser, Debug)]
struct Args {
    /// Calibration Value
    #[arg(short= 'c')]
    calibration_value_ns: Option<f64>,

    /// Max runtime in ms
    #[arg(short= 't')]
    max_time_ms: Option<u64>,

    /// Periodic Tasks to emulate, ordered in decreasing priority, separated by
    /// spaces, only client.
    ///
    /// Task format: [<uplink_us>,<runtime_us>,<downlink_us>,<deadline_us>,<period_us>]
    #[arg(value_parser= parse_periodic_task)]
    taskset: Option<Vec<PeriodicTaskOffloaded>>,

    /// Number of tasks, only server
    #[arg(short= 'N')]
    taskset_size: Option<usize>,

    /// Server Address.
    #[arg(short='a', long= "addr")]
    address: String,

    /// Server Port.
    #[arg(short='p', long= "port")]
    port: u16,

    /// Start as server (if specified) or client.
    #[arg(short= 'S', action= clap::ArgAction::SetTrue)]
    is_server: bool,

    /// Use SCHED_OTHER instead of SCHED_FIFO
    #[arg(long= "sched-normal", action= clap::ArgAction::SetTrue)]
    non_rt: bool,
}

fn main() {
    let args = <Args as clap::Parser>::parse();

    let ip_addr = <std::net::IpAddr as std::str::FromStr>::from_str(&args.address).unwrap();
    let address = std::net::SocketAddr::new(ip_addr, args.port);

    assert!(args.taskset.is_some() || args.taskset_size.is_some());
    assert!(args.taskset.is_none() || args.taskset_size.is_none());
    assert!(args.taskset.is_none() == args.is_server);

    let num_tasks = args.taskset.as_ref().map_or_else(|| args.taskset_size.unwrap(), |t| t.len());

    if num_tasks > 97 {
        panic!("Too many tasks specified");
    }

    let signal = TerminateSignal::new();
    let calibration_value = args.calibration_value_ns.map(|t| Time::nanos(t));
    let max_time = args.max_time_ms.map(|t| Time::millis(t as f64));

    let mut ctrlc_signal = signal.clone();
    pid::set_handler(pid::Signal::SIGINT, move |_| { ctrlc_signal.set(true); }).unwrap();

    if !args.non_rt {
        let now = clock::Monotonic::now();
        let timeout = Time::secs(5.0);
        loop {
            let res =
                pid::set_sched_policy(
                    Pid(std::process::id()),
                    pid::SchedPolicy::FIFO(99),
                    pid::SchedFlags::empty()
                );

            if res.is_ok() {
                break;
            }

            if (clock::Monotonic::now() - now) > timeout {
                res.unwrap();
                break;
            }

            clock::Monotonic::sleep_relative(Time::millis(99.0));
        };
    }

    let now = clock::Monotonic::now();
    let delta = Time::millis(500.0);
    let start_time = now + delta;

    let threads: Vec<_> =
        if args.is_server {
            let (producer, consumers) = PTOServerProducer::new(num_tasks, address, calibration_value);

            std::iter::once_with(|| {
                start_rt_thread(move |signal| {
                    eprintln!("Starting rt thread of PID {}, Producer", std::process::id());

                    producer.run(start_time, max_time, signal);
                    Ok(String::with_capacity(0))
                }, if args.non_rt { None } else { Some(98) }, signal.clone())
            })
            .chain(consumers.into_iter().enumerate()
                .map(|(i, consumer)| {
                    let rt_prio = 97 - i as i32;

                    start_rt_thread(move |signal| {
                        eprintln!("Starting rt thread of PID {}, Consumer {}", std::process::id(), i);

                        consumer.run(start_time, max_time, signal);
                        Ok(String::with_capacity(0))
                    }, if args.non_rt { None } else { Some(rt_prio) }, signal.clone())
                }))
            .collect()
        } else {
            let mut taskset = args.taskset.unwrap();
            taskset.iter_mut().enumerate()
                .for_each(|(i, task)| {
                    task.name = format!("Task{i:03}");
                    task.calibration_value = calibration_value;
                });

            taskset.into_iter().enumerate()
            .map(|(i, task)| {
                let rt_prio = 97 - i as i32;

                let client = PTOClient::new(task, i as u32, address);
                start_rt_thread(move |signal| {
                    eprintln!("Starting rt thread of PID {}, task {}", std::process::id(), client.name());

                    let mut data = String::new();
                    client.run(start_time, max_time, signal, &mut data);
                    Ok(data)
                }, if args.non_rt { None } else { Some(rt_prio) }, signal.clone())
            })
            .collect()
        };

    let mut data = String::new();
    PTOResultWriter::write_header(&mut data);
    print!("{data}");
    for thread in threads {
        let data = thread.join().unwrap().unwrap();
        print!("{data}");
        std::io::Write::flush(&mut std::io::stdout()).unwrap();
    }
}

fn parse_periodic_task(data: &str) -> anyhow::Result<PeriodicTaskOffloaded> {
    let data = data.trim()
        .strip_prefix("[").and_then(|d| d.strip_suffix("]"))
        .ok_or_else(|| anyhow::format_err!("Missing [ or ] delimiters"))?;

    let piecies: Vec<&str> = data.split(',').collect();
    if piecies.len() != 5 {
        return Err(anyhow::format_err!("Invalid task format"));
    }

    Ok(PeriodicTaskOffloaded {
        name: String::with_capacity(0),
        uplink: Time::micros(piecies[0].parse()?),
        runtime: Time::micros(piecies[1].parse()?),
        downlink: Time::micros(piecies[2].parse()?),
        deadline: Time::micros(piecies[3].parse()?),
        period: Time::micros(piecies[4].parse()?),
        calibration_value: None,
    })
}
