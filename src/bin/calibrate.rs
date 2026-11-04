use rt_synth::prelude::*;

/// Calibrates the system by running a cpuhog loop for a specified number of iterations
/// and returns a calibration value to be used in the other binaries.
#[derive(clap::Parser, Debug)]
struct Args {
    #[arg(default_value= "10000000")]
    calibration_iterations: usize,
}

fn main() {
    let args = <Args as clap::Parser>::parse();

    let calibration_value = calibrate(args.calibration_iterations);
    println!("Calibration value: {:.3}", calibration_value.as_nanos());
}