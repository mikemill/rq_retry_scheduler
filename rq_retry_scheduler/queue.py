from datetime import datetime
import logging
import rq

from . import util


logger = logging.getLogger('rq:retryscheduler:queue')


class Queue(rq.Queue):
    """
    A drop in replacement for rq.Queue that adds in the scheduling enqueueing
    options.
    """
    scheduler_jobs_key = 'rq:retryscheduler:scheduled_jobs'
    current_time = datetime.utcnow  # This is to make it easy to test

    def schedule_job(self, job, enqueue_dt):
        logger.info('Queuing job {0:s} to run at {1:s}'.format(
            job.id, str(enqueue_dt)))

        self.connection.zadd(
            self.scheduler_jobs_key, {job.id: util.to_unix(enqueue_dt)})

        job.meta['scheduled_for'] = enqueue_dt

        job.save()

    def enqueue_at(self, scheduled_at, func, *args, **kwargs):
        meta = {'enqueue_at': scheduled_at}
        return self.enqueue(func, args=args, kwargs=kwargs, meta=meta)

    def enqueue_in(self, time_delta, func, *args, **kwargs):
        meta = {'enqueue_at': self.current_time() + time_delta}
        return self.enqueue(func, args=args, kwargs=kwargs, meta=meta)

    def enqueue_job_at(self, scheduled_at, job):
        job.meta['enqueue_at'] = scheduled_at
        return self.enqueue_job(job)

    def enqueue_job_in(self, time_delta, job):
        job.meta['enqueue_at'] = self.current_time() + time_delta
        return self.enqueue_job(job)

    def enqueue_job(self, job, pipeline=None, at_front=False):
        enqueue_at = job.meta.pop('enqueue_at', None)

        if not enqueue_at:
            return super(Queue, self).enqueue_job(job, pipeline, at_front)

        self.schedule_job(job, enqueue_at)

        return job

    def repeat_job(self, job, interval, max_runs=None):
        job.meta.update({
            'interval': interval,
            'max_runs': max_runs,
            'run_count': 0,
        })

        if job not in self:
            self.enqueue_job_in(interval, job)
        else:
            job.save()

        return job

    def scheduled_jobs(self):
        num_jobs = self.connection.zcard(self.scheduler_jobs_key)
        job_ids = self.connection.zrange(
            self.scheduler_jobs_key, 0, num_jobs)

        for job_id in job_ids:
            job = self.job_class.fetch(
                job_id.decode('utf-8'), connection=self.connection)

            if job.origin == self.name:
                yield job

    def __contains__(self, job):
        try:
            job_id = job.id
        except AttributeError:
            job_id = job

        return self.connection.zrank(
            self.scheduler_jobs_key, job_id) is not None

    def unschedule_job(self, job):
        try:
            job_id = job.id
        except AttributeError:
            job_id = job

        self.connection.zrem(self.scheduler_jobs_key, job_id)
