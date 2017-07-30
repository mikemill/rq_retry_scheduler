#!/usr/bin/env python

from __future__ import print_function

import click
import logging
from rq.cli import cli as rqcli
from rq.cli import helpers
from rq.utils import ColorizingStreamHandler

from rq_retry_scheduler import Scheduler


@click.group()
def main():
    """RQ Retry Scheduler command line tool."""


@main.command()
@rqcli.url_option
@rqcli.config_option
@click.option('--burst', '-b', is_flag=True,
              help="Burst Mode. Move any jobs and quit")
@click.option('--interval', '-i', type=float, default=10.0,
              help="Scheduler polling interval (in seconds)")
@click.option('--loglevel', help="Logging level", default='INFO',
              type=click.Choice([
                  'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']))
def run(url, config, burst, interval, loglevel):
    conn = rqcli.connect(url, config)
    setup_logging(loglevel)
    scheduler = Scheduler(connection=conn, interval=interval)
    scheduler.run(burst)


@main.command()
@rqcli.url_option
@rqcli.config_option
@click.option('--rq/--no-rq', default=True, help="Show RQ info data")
@click.pass_context
def info(ctx, url, config, rq):
    conn = rqcli.connect(url, config)

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
