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
    /// spaces
    ///
    /// Task format: [<runtime_us>,<deadline_us>,<period_us>]
    #[arg(value_parser= parse_periodic_task)]
    taskset: Vec<PeriodicTask>,
}

fn main() {
    let mut args = <Args as clap::Parser>::parse();

    if args.taskset.len() > 99 {
        panic!("Too many tasks specified");
    }

    let signal = TerminateSignal::new();
    let calibration_value = args.calibration_value_ns.map(|t| Time::nanos(t));
    let max_time = args.max_time_ms.map(|t| Time::millis(t as f64));

    args.taskset.iter_mut().enumerate()
        .for_each(|(i, task)| {
            task.name = format!("Task{i:03}");
            task.calibration_value = calibration_value;
        });


    let mut ctrlc_signal = signal.clone();
    pid::set_handler(pid::Signal::SIGINT, move |_| { ctrlc_signal.set(true); }).unwrap();

    let now = clock::Monotonic::now();
    let delta = Time::millis(500.0);
    let start_time = now + delta;

    let threads: Vec<_> =
        args.taskset.into_iter().enumerate()
        .map(|(i, task)| {
            let rt_prio = 99 - i as i32;

            start_rt_thread(move |signal| {
                eprintln!("Starting rt thread of PID {}, task {}", std::process::id(), task.name);

                let mut data = String::new();
                task.run(start_time, max_time, signal, &mut data);
                Ok(data)
            }, Some(rt_prio), signal.clone())
        })
        .collect();

    let mut data = String::new();
    // ResultWriter::write_header(&mut data);
    print!("{data}");
    for thread in threads {
        let data = thread.join().unwrap().unwrap();
        print!("{data}");
        std::io::Write::flush(&mut std::io::stdout()).unwrap();
    }
}

fn parse_periodic_task(data: &str) -> anyhow::Result<PeriodicTask> {
    let data = data.trim()
        .strip_prefix("[").and_then(|d| d.strip_suffix("]"))
        .ok_or_else(|| anyhow::format_err!("Missing [ or ] delimiters"))?;

    let piecies: Vec<&str> = data.split(',').collect();
    if piecies.len() != 3 {
        return Err(anyhow::format_err!("Invalid task format"));
    }

    Ok(PeriodicTask {
        name: String::with_capacity(0),
        runtime: Time::micros(piecies[0].parse()?),
        deadline: Time::micros(piecies[1].parse()?),
        period: Time::micros(piecies[2].parse()?),
        calibration_value: None,
    })
}
