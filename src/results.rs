pub mod prelude {
    pub use super::{
        ResultWriter,
        TaskResultData,
        JobResultData,
    };
}

pub trait TaskResultData<const TN: usize> {
    fn header() -> [&'static str; TN];

    fn write_data<W: std::fmt::Write>(&self, writer: W) -> Result<(), std::fmt::Error>;
}

pub trait JobResultData<const JN: usize, Task> {
    fn header() -> [&'static str; JN];

    fn write_data<W: std::fmt::Write>(&self, task: &Task, writer: W) -> Result<(), std::fmt::Error>;
}

pub struct ResultWriter<const TN: usize, const JN: usize, Task: TaskResultData<TN>, Job: JobResultData<JN, Task>> {
    task: Task,
    task_num: u64,
    results: Vec<Job>,
}

impl<const TN: usize, const JN: usize, Task: TaskResultData<TN>, Job: JobResultData<JN, Task>>
    ResultWriter<TN, JN, Task, Job>
{
    pub fn new(task: Task, task_num: u64) -> Self {
        Self { task, task_num, results: Vec::new() }
    }

    pub fn push(&mut self, data: Job) {
        self.results.push(data);
    }

    pub fn len(&self) -> usize {
        self.results.len()
    }

    pub fn write_header<W: std::fmt::Write>(mut writer: W) -> Result<(), std::fmt::Error> {
        write!(writer, "TaskNum,JobNum")?;
        for field in Task::header() {
            write!(writer, ",{}", field)?;
        }
        for field in Job::header() {
            write!(writer, ",{}", field)?;
        }
        write!(writer, "\n")?;
        Ok(())
    }

    pub fn write_back<W: std::fmt::Write>(self, mut writer: W) -> Result<(), std::fmt::Error> {
        for (job_num, job) in self.results.into_iter().enumerate() {
            write!(&mut writer, "{},{}", self.task_num, job_num)?;
            self.task.write_data(&mut writer)?;
            job.write_data(&self.task, &mut writer)?;
            write!(writer, "\n")?;
        }

        Ok(())
    }
}
