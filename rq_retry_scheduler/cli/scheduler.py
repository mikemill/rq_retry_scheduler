#!/usr/bin/env python

from __future__ import print_function

import argparse
import logging
import os
from redis import StrictRedis
from rq.utils import ColorizingStreamHandler

from rq_retry_scheduler import Scheduler


def get_arguments(args=None):
    parser = argparse.ArgumentParser(description='RQ Retry Scheduler')

    # Redis connection
    parser.add_argument(
        '-H', '--host', help='Redis host',
        default=os.environ.get('RQ_REDIS_HOST', 'localhost'))

    parser.add_argument(
        '-p', '--port', help='Redis port', type=int,
        default=int(os.environ.get('RQ_REDIS_PORT', 6379)))

    parser.add_argument(
        '-d', '--db', help='Redis database', type=int,
        default=int(os.environ.get('RQ_REDIS_DB', 0)))

    parser.add_argument(
        '-P', '--password', help='Redis password',
        default=os.environ.get('RQ_REDIS_PASSWORD'))

    parser.add_argument(
        '-u', '--url',
        default=os.environ.get('RQ_REDIS_URL'),
        help='URL describing Redis connection details. '
             'Overrides other connection arguments if supplied.')

    parser.add_argument(
        '-b', '--burst', action='store_true',
        help='Burst mode. Move any jobs and quit')

    parser.add_argument(
        '-i', '--interval', help='Scheduler polling interval (in seconds)',
        default=10.0, type=float)

    parser.add_argument('--info', help="Display information and then quit",
                        action="store_true")

    parser.add_argument('--loglevel', help="Logging level",
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR',
                                 'CRITICAL'])

    return parser.parse_args(args)


def get_redis(args):
    if args.url:
        connection = StrictRedis.from_url(args.url)
    else:
        connection = StrictRedis(args.host, args.port, args.db, args.password)

    return connection


def setup_logging(args):
    logger = logging.getLogger('rq:retryscheduler:scheduler')
    logger.setLevel(vars(args).get('loglevel', 'INFO'))
    formatter = logging.Formatter(fmt='%(asctime)s %(message)s',
                                  datefmt='%H:%M:%S')
    handler = ColorizingStreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def info(scheduler):
    """Show some stats and then quit"""
    jobs = scheduler.schedule()
    num_jobs = len(jobs)
    print("Number of jobs scheduled:", num_jobs)

    if num_jobs:
        next_job = jobs[0][1]
        print("Next job to be queued at:", next_job)


def main():
    args = get_arguments()
    setup_logging(args)
    connection = get_redis(args)
    scheduler = Scheduler(connection=connection, interval=args.interval)

    if args.info:
        info(scheduler)
    else:
        scheduler.run(args.burst)
