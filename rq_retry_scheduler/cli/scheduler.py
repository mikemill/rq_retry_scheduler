#!/usr/bin/env python

from __future__ import print_function

import click
from datetime import datetime
from functools import partial
import logging
from redis import Redis
from rq.cli import cli as rqcli
from rq.cli import helpers
from rq.utils import ColorizingStreamHandler

from rq_retry_scheduler import Scheduler


url_option = click.option('--url', '-u', envvar='RQ_REDIS_URL',
                          help='URL describing Redis connection details.')
config_option = click.option('--config', '-c', envvar='RQ_CONFIG',
                             help='Module containing RQ settings.')


def connect(url, config=None, connection_class=Redis):
    if url:
        return connection_class.from_url(url)

    settings = helpers.read_config_file(config) if config else {}
    return helpers.get_redis_from_config(settings, connection_class)


@click.group()
def main():
    """RQ Retry Scheduler command line tool."""


@main.command()
@url_option
@config_option
@click.option('--burst', '-b', is_flag=True,
              help="Burst Mode. Move any jobs and quit")
@click.option('--interval', '-i', type=float, default=10.0,
              help="Scheduler polling interval (in seconds)")
@click.option('--loglevel', help="Logging level", default='INFO',
              type=click.Choice([
                  'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']))
def run(url, config, burst, interval, loglevel):
    """Run the RQ Retry Scheduler"""
    conn = connect(url, config)
    setup_logging(loglevel)
    scheduler = Scheduler(connection=conn, interval=interval)
    scheduler.run(burst)


@main.command()
@url_option
@config_option
@click.option('--rq/--no-rq', default=True, help="Show RQ info data")
@click.pass_context
def info(ctx, url, config, rq):
    """Get information about the RQ Schedule"""
    conn = connect(url, config)

    if rq:
        ctx.invoke(rqcli.info, url=url, config=config)
        click.echo('')

    scheduler = Scheduler(connection=conn)
    jobs = scheduler.schedule()
    num_jobs = len(jobs)

    termwidth, _ = click.get_terminal_size()
    chartwidth = min(20, termwidth - 20)

    scale = helpers.get_scale(num_jobs)
    ratio = chartwidth * 1.0 / scale

    chart = helpers.green('|' + '█' * int(ratio * num_jobs))
    line = 'Scheduled: {:s} {:d}'.format(chart, num_jobs)
    click.echo(line)

    if num_jobs:
        next_job = jobs[0][1]
        click.echo("Next job to be queued at: {:s}".format(str(next_job)))


@main.command(name='list')
@url_option
@config_option
def list_jobs(url, config):
    """List out all the scheduled jobs"""
    conn = connect(url, config)
    scheduler = Scheduler(connection=conn)

    now = datetime.utcnow()

    jobs = scheduler.schedule(fetch_jobs=True)

    if jobs:
        queue_name_length = max(10, *(len(job.origin) for job, _ in jobs))

        cyan = partial(click.style, fg='cyan')

        width = queue_name_length + len(cyan(''))

        click.echo('{: ^19} {: ^36} {: ^{width}}   {:s}'.format(
            'Queue Time', 'Job ID', 'Queue Name', 'Description',
            width=queue_name_length))

        for job, time in jobs:
            if time <= now:
                color = helpers.red
            else:
                color = helpers.green

            line = '{:s} {:s} {: <{width}}   {:s}'.format(
                color(str(time)), job.id,
                cyan(job.origin), job.description, width=width)
            click.echo(line)
    else:
        click.echo("No jobs scheduled")


main.add_command(rqcli.main, name='rq')


def setup_logging(loglevel):
    logger = logging.getLogger('rq:retryscheduler:scheduler')
    logger.setLevel(loglevel)
    formatter = logging.Formatter(fmt='%(asctime)s %(message)s',
                                  datefmt='%H:%M:%S')
    handler = ColorizingStreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
