[![Build Status](https://travis-ci.org/mikemill/rq_retry_scheduler.svg?branch=master)](https://travis-ci.org/mikemill/rq_retry_scheduler)
[![Coverage Status](https://coveralls.io/repos/github/mikemill/rq_retry_scheduler/badge.svg?branch=master)](https://coveralls.io/github/mikemill/rq_retry_scheduler?branch=master)

# RQ Retry Scheduler

[RQ Retry Scheduler](https://github.com/mikemill/rq_retry_scheduler) extends the RQ worker class to provide functionality to enqueue jobs based on time.

Additionally, it provides a worker that will automatically retry failed jobs using a backoff scheme.

# Usage

## Queues

The `rq_retry_scheduler.Queue` class can be used as a drop in replacement for `rq.Queue`.  It provides two additional enqueuing methods:

* `enqueue_at` - Enqueue the job at the specified time.  The time should be a `datetime.datetime` object with the UTC timezone.
* `enqueue_in` - Enqueue the job after the specified amount of time.  The time should be a `datetime.timedelta` object.

Additionally, two other methods are available `enqueue_job_at` and `enqueue_job_in` which work like the above methods but take a `rq.Job` object as the paramter.

### Getting list of jobs

`Queue.scheduled_jobs` is a generator that produces the jobs that are currently scheduled for a particular queue.  
`Scheduler.schedule` returns a list of job ids and scheduled time.

### Checking if a job is scheduled

You can check if a job or job id is currently scheduled.  Example: `job in queue`

### Unscheduling jobs

You can unschedule jobs with `Queue.remove_job`.  This removes the job from the scheduler queue but does not remove it from RQ.

### Repeating jobs

Once a job has been enqueued you can set it to repeat with `Queue.repeat` which takes the job and a `datetime.timedelta` object.  Additionally you can
pass a `max_runs` value to limit the number of times it will repeat.

If the job is not already in the queue it will be enqueued at the given interval.

In order to ensure that job results cleanup doesn't remove the job (thus breaking the repetition) the job is copied into a new job.
The parent job can be accessed via `Job.parent` for the repeated jobs.

Due to how the jobs are put into the work queues the maximum frequency is controlled by the scheduler's interval.

## Scheduler

In order to move jobs from the schedule queue into the proper RQ queue a scheduler needs to be ran.
This can be accomplished via the `rqscheduler` script.  Additionally, you can extend from `rq_retry_scheduler.Scheduler` to customize the functonality.

## Worker

A worker is provided that will requeue jobs that fail using a defiend backoff strategy.
The worker is designed to use the functionality from the `Queue` class to control the backoff.

The worker can be used by using the `-w` option to `rq worker`: `rq worker -w 'rq_rety_scheduler.Worker`.

The backoff times and total attempts is controlled by `Worker.retry_delays`.


# Installation

Install via pip

```
pip install rq-retry-scheduler
```


# Acknowledgements

This package was based heavily on the work of [RQ Scheduler](https://github.com/ui/rq-scheduler).
