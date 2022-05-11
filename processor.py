import asyncio
import itertools
import logging
import os
import random
import time
from functools import partial
from multiprocessing import get_context
from statistics import mean

from utils import get_settings

loop = None


def get_event_loop():
    global loop
    if not loop:
        loop = asyncio.get_event_loop_policy().new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


def start_task_pool(engine, func, idx, **kwargs):
    procs = get_settings().proc_concurrency
    concurrency = kwargs.pop("concurrency", procs)
    slot = f'{idx}'

    ts = time.time()
    loop = get_event_loop()
    results = []
    try:
        if engine == 'vespa':
            from db.vespa_service import get_connection
            conn = get_connection(get_settings().cluster_settings.connection_string)
        else:
            from db.elasticsearch_service import get_connection
            conn = get_connection(get_settings().cluster_settings.connection_string)
        results = loop.run_until_complete(
            asyncio.gather(
                *[func(slot, subslot, concurrency, conn, **kwargs) for subslot in range(concurrency)]
            )
        )
    except Exception as ex:
        logging.exception(ex)
    logging.info(f"ASYNC POOL OPERATION TIME: {time.time() - ts}")
    return results


def init_worker_process(engine, builder):
    random.seed(os.getpid())
    if engine == 'vespa':
        from db.vespa_service import get_connection
        get_connection(get_settings().cluster_settings.connection_string)
    else:
        from db.elasticsearch_service import get_connection
        get_connection(get_settings().cluster_settings.connection_string)


async def start_processors(engine, max_proc, builder, func, **kwargs):
    """
    plan: use ProcessorPool to use all cpus + assign chunks of work
    each chunk of work inside is done by async workers inside processor main thread
    :return:
    """
    await builder.run_init_jobs()
    max_workers = max_proc
    # task_repeat = repeat if repeat else max_workers

    logging.info(f"STARTING WITH N_WORKERS: {max_workers}")
    with get_context("spawn").Pool(initializer=partial(init_worker_process, engine, builder),
                                   processes=max_workers) as pool:
        start_ts = time.time()
        result = pool.map(partial(start_task_pool, engine, func, **kwargs), range(max_workers))
        print(result)
    total_runtime = time.time() - start_ts
    print(
        result)  # [[47.84888879, 48.020549, 47.360947229999994, 47.131018299999994, 47.39814992], [47.50742029, 47.88334506, 46.89231461, 47.57275019, 48.03460203]]
    logging.info("worker pool terminated")
    mean_ts = mean(itertools.chain(*result)) if result and result[0] else 0
    return total_runtime, mean_ts
