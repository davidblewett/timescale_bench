# -*- coding: utf-8 -*-

"""Console script for timescale_bench."""
import csv
import statistics
import sys
import click
from datetime import datetime, timedelta
from multiprocessing import BoundedSemaphore, cpu_count
from timescale_bench.worker_queue import WorkerQueue


def flush_batch(records, worker_queues):
    for record in records:
        # Ensure jobs for the same host get pushed onto the same queue
        index = hash(record[0]) % len(worker_queues)
        worker_queues[index].put(record)

    for queue in worker_queues:
        for result in queue.join():
            yield result


@click.command()
@click.option('--concurrency', '-c',
              default=4,
              metavar='QUEUE_DEPTH',
              type=click.IntRange(min=1, max=16, clamp=True),
              help='Number of queues to process concurrently')
@click.option('--dsn',
              default=None,
              required=True,
              envvar='DSN',
              help='Connection arguments specified using as a single string in the following format: postgres://user:pass@host:port/database')
@click.option('--jobs', '-j',
              default=cpu_count(),
              metavar='CORES',
              type=click.IntRange(min=1, max=cpu_count(), clamp=True),
              help='Process data across N cores.')
@click.option('--file-input', '-f',
              required=True,
              type=click.File('r'),
              help='Path to file to read from, or - for stdin')
@click.option('--file-output', '-o',
              type=click.File('w'),
              help='Path to file to write host per-minute results to')
@click.option('--skip-header/--no-skip-header',
              default=True,
              help='Path to file to write host per-minute results to')
def main(concurrency, dsn, jobs, file_input, file_output, skip_header):
    """Console script for timescale_bench."""
    exit_code = 0
    exec_sem = BoundedSemaphore(jobs)
    worker_queues = [
        WorkerQueue(dsn, exec_sem)
        for i in range(concurrency)
    ]

    reader = csv.reader(file_input)
    if file_output:
        writer = csv.writer(file_output)
    if skip_header:
        next(reader, None)

    try:
        results = list(flush_batch(reader, worker_queues))
        if not results:
            raise RuntimeError('No input provided')
        if file_output:
            for query_result in results:
                for result in query_result.results:
                    writer.writerow(result)
        timing = dict(
            total=sum((res.duration for res in results)),
            shortest=min((res.duration for res in results)),
            median=statistics.median_high((res.duration for res in results)),
            avg=statistics.mean((res.duration for res in results)),
            longest=max((res.duration for res in results)),
        )
        # Convert to timedelta for display
        timing = {
            key: timedelta(seconds=value)
            for key, value in timing.items()
        }
        click.echo(f"Number of queries processed: {len(results)}")
        click.echo(f"      Total processing time: {timing['total']}")
        click.echo(f"        Shortest query time: {timing['shortest']}")
        click.echo(f"          Median query time: {timing['median']}")
        click.echo(f"               Average time: {timing['avg']}")
        click.echo(f"         Longest query time: {timing['longest']}")
    except Exception as err:
        click.echo(f'Failure: {err}')
        exit_code = 1
    finally:
        return exit_code


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
