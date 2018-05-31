import psycopg2
from collections import deque, namedtuple
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, wait
from time import perf_counter


QueryResult = namedtuple('QueryResult', ['duration', 'results'])


# This is mutated by subprocesses at runtime
_connection = None
_sql = """
    SELECT
        host,
        date_trunc('minute', ts) AS time_slice,
        MIN(usage) AS min_cpu_usage,
        MAX(usage) AS max_cpu_usage
    FROM cpu_usage
    WHERE
        host = %s AND
        ts BETWEEN %s AND %s
    GROUP BY
        host,
        date_trunc('minute', ts)
"""


def _exec_sql(dsn, record):
    """Your tool should take the CSV row values hostname, start time, end time and use them to
    generate a SQL query for each row that returns the max cpu usage and min cpu usage of the
    given hostname for every minute in the time range specified by the start time and end time.
    """
    # This function is only ever executed in a subprocess;
    # here we cache the connection for the life of the subprocess.
    global _connection
    if _connection is None:
        _connection = psycopg2.connect(dsn)
    begin = perf_counter()
    with _connection.cursor() as cursor:
        cursor.execute(_sql, record)
        results = cursor.fetchall()
    return QueryResult(perf_counter()-begin, results)


class WorkerQueue(object):

    def __init__(self, dsn, semaphore):
        self.dsn = dsn
        self.jobs = deque()
        self.pool = ProcessPoolExecutor()
        self.semaphore = semaphore

    def _flush(self):
        done, not_done = wait(self.jobs)
        for job in done:
            yield job.result()
        if not_done:
            raise RuntimeError(f"Jobs didn't complete: {not_done}")
        self.jobs.clear()

    def put(self, record):
        self.semaphore.acquire()
        job = self.pool.submit(_exec_sql, self.dsn, record)
        job.add_done_callback(lambda fut: self.semaphore.release())
        self.jobs.append(job)
        return job

    def join(self):
        for result in self._flush():
            yield result
