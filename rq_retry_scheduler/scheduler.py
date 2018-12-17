from copy import deepcopy
from datetime import datetime
import logging
import time

from rq.connections import resolve_connection
from rq.exceptions import NoSuchJobError
from rq.job import Job

from .queue import Queue
from .util import to_unix


logger = logging.getLogger('rq:retryscheduler:scheduler')


class Scheduler(object):
    scheduler_key = 'rq:retryscheduler'
    scheduler_jobs_key = scheduler_key + ':scheduled_jobs'
    current_time = datetime.utcnow  # To make unit testing easier

    def __init__(self, job_class=None, queue_class=None, interval=1,
                 connection=None):
        self.interval = interval
        self.log = logger
        self.connection = resolve_connection(connection)
        self.job_class = job_class if job_class is not None else Job
        self.queue_class = queue_class if queue_class is not None else Queue
        self._queue_cache = {}

    def remove_job(self, job_id):
        self.log.debug("Removing job {} from scheduled list".format(job_id))
        self.connection.zrem(self.scheduler_jobs_key, job_id)

    def get_queue(self, queue_name):
        if queue_name not in self._queue_cache:
            self._queue_cache[queue_name] = self.queue_class(
                queue_name, connection=self.connection)

        return self._queue_cache[queue_name]

    def get_job(self, job_id):
        if isinstance(job_id, bytes):
            job_id = job_id.decode('utf-8')

        return self.job_class.fetch(job_id, connection=self.connection)

    def get_jobs_to_queue(self, score):
        job_ids = self.connection.zrangebyscore(
            self.scheduler_jobs_key, 0, score)

        for job_id in job_ids:
            try:
                self.log.debug(
                    "Job {} is ready to be put in the queue".format(job_id))
                yield self.get_job(job_id)
            except NoSuchJobError:
                self.remove_job(job_id)

    def delay_job(self, job, time_delta):
        amount = int(time_delta.total_seconds())
        self.connection.zincrby(self.scheduler_jobs_key, amount, job.id)

    def should_repeat_job(self, job):
        max_runs = job.meta['max_runs']
        run_count = job.meta['run_count']

        return max_runs is None or max_runs > run_count

    def repeat_job_id(self, job):
        return 'repeat_{}_{}'.format(job.id, job.meta.get('run_count', 0) + 1)

    def make_repeat_job(self, job):
        meta = deepcopy(job.meta)
        meta.pop('interval', None)
        meta.pop('run_count', None)
        meta.pop('max_runs', None)

        params = {
            'func': job.func,
            'args': job.args,
            'kwargs': job.kwargs,
            'connection': job.connection,
            'result_ttl': job.result_ttl,
            'ttl': job.ttl,
            'id': self.repeat_job_id(job),
            'origin': job.origin,
            'meta': meta,
        }

        repeat_job = Job.create(**params)
        repeat_job.parent = job
        repeat_job.save()

        return repeat_job

    def handle_job_repeat(self, job, queue):
        repeat_job = self.make_repeat_job(job)
        self.log.info("Enqueuing job {} to queue {}".format(
                      repeat_job.id, repeat_job.origin))

        queue.enqueue_job(repeat_job)

        job.meta['run_count'] += 1

        if self.should_repeat_job(job):
            self.log.info("Scheduling job {} to repeat in {}".format(
                          job.id, job.meta['interval']))
            self.delay_job(job, job.meta['interval'])
            job.save()
        else:
            self.log.info("Removing job {} from scheduler".format(job.id))
            self.remove_job(job.id)

        return repeat_job

    def is_repeat(self, job):
        return 'interval' in job.meta

    def enqueue_jobs(self):
        self.log.debug('Checking for scheduled jobs...')

        jobs = self.get_jobs_to_queue(to_unix(self.current_time()))

        for job in jobs:
            queue = self.get_queue(job.origin)
            if self.is_repeat(job):
                self.handle_job_repeat(job, queue)
            else:
                self.log.info(
                    "Enqueuing job {} to queue {}".format(job.id, job.origin))
                queue.enqueue_job(job)
                self.remove_job(job.id)

    def schedule(self, fetch_jobs=False):
        """Returns the job ids and when they are scheduled to be queued"""
        data = self.connection.zrange(
            self.scheduler_jobs_key, 0, -1, withscores=True)

        if fetch_jobs:
            return [
                (self.get_job(job_id), datetime.utcfromtimestamp(ts))
                for job_id, ts in data]
        else:
            return [
                (job_id, datetime.utcfromtimestamp(ts))
                for job_id, ts in data]

    def run(self, burst=False):
        self.log.info('Starting RQ Retry Scheduler..')

        try:
            while True:
                self.enqueue_jobs()

                if burst:
                    break

                time.sleep(self.interval)
        finally:
            pass
