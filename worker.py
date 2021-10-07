import asyncio
import json
import logging
import time
from functools import partial
from multiprocessing import get_context

import uvicorn
from fastapi import FastAPI, Request

from create_data import Data1Builder
from elasticsearch_service import get_es_connection
from utils import get_settings

app = FastAPI()

loop = None


def get_event_loop():
    global loop
    if not loop:
        loop = asyncio.get_event_loop_policy().new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


def start_task_pool(func, idx, **kwargs):
    procs = get_settings().proc_concurrency
    concurrency = kwargs.pop("concurrency", procs)
    slot = f'{idx + 1}'

    ts = time.time()
    loop = get_event_loop()
    results = []
    try:
        es = get_es_connection()
        results = loop.run_until_complete(
            asyncio.gather(
                *[func(slot, es, **kwargs) for _ in range(concurrency)]
            )
        )
    except Exception as ex:
        logging.exception(ex)
    logging.info(f"ASYNC POOL OPERATION TIME: {time.time() - ts}")
    return results


async def start_processors(max_proc, repeat, func, **kwargs):
    """
    plan: use ProcessorPool to use all cpus + assign chunks of work
    each chunk of work inside is done by async workers inside processor main thread
    :return:
    """

    max_workers = min(get_settings().num_proc, max_proc) if max_proc > 0 else get_settings().num_proc
    task_repeat = repeat if repeat else max_workers

    logging.info(f"STARTING WITH N_WORKERS: {max_workers}, REPEAT: {task_repeat}")
    with get_context("spawn").Pool(initializer=get_es_connection, processes=max_workers) as pool:
        result = pool.map(partial(start_task_pool, func, **kwargs), range(task_repeat))

    logging.info("worker pool terminated")


# , settings: Settings = Depends(get_settings)
@app.post("/read-queue")
async def read_queue(request: Request):
    """
    """

    body = await request.json()
    if body is None:
        # Expect application/json request
        response = "", 415
    else:
        try:
            if 'TopicArn' in body and 'Message' in body:
                message = body['Message']
            else:
                message = body

            cmd = message['cmd']  # search,index,create
            cmd_args = None
            try:
                cmd_args = json.loads(message['cmd_args'])  # {'doc_nb': 1000}
            except:
                pass
            max_workers = message['max_workers']  # -1,2,3,...
            profile = message['profile']  # '1'
            repeat = message.get('repeat', None)  # 1

            func = None
            concurrency = get_settings().proc_concurrency
            if profile == '1':
                builder = await Data1Builder.new(index='index1')
                if cmd == 'build_data':
                    func = builder.build_data_repo
                elif cmd == 'create_index':
                    func = builder.create_index
                    concurrency = 1
                elif cmd == 'index_docs':
                    func = builder.index_docs
                elif cmd == 'search':
                    func = builder.run_queries
                await start_processors(max_workers, repeat, func, concurrency=concurrency, **cmd_args)

            response = "", 200
        except Exception as ex:
            logging.exception('Error processing message: %s' % ex)
            response = ex, 500

    return response


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
