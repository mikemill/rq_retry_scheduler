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

    def enqueue_jobs(self):
        self.log.info('Checking for scheduled jobs...')

        jobs = self.get_jobs_to_queue(to_unix(self.current_time()))

        for job in jobs:
            self.log.info(
                "Enqueuing job {} to queue {}".format(job.id, job.origin))
            queue = self.get_queue(job.origin)
            queue.enqueue_job(job)
            self.remove_job(job.id)

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
