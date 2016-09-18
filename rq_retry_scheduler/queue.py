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

        self.connection._zadd(
            self.scheduler_jobs_key, util.to_unix(enqueue_dt), job.id)

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
            return super().enqueue_job(job, pipeline, at_front)

        self.schedule_job(job, enqueue_at)

        return job
