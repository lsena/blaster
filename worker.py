import asyncio
import itertools
import json
import logging
import os
import random
import threading
import time
from functools import partial
from multiprocessing import get_context
from statistics import mean

import aioboto3
import uvicorn
from azure.storage.blob import BlobServiceClient
from fastapi import FastAPI, Request

from builders.es.data1 import Data1Builder
from builders.es.data2 import Data2Builder
from builders.es.data3 import Data3Builder
from builders.vespa.data1 import VespaData1Builder
from utils import get_settings

app = FastAPI()

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
    slot = f'{idx + 1}'

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


async def start_processors(engine, max_proc, repeat, builder, func, **kwargs):
    """
    plan: use ProcessorPool to use all cpus + assign chunks of work
    each chunk of work inside is done by async workers inside processor main thread
    :return:
    """
    await builder.run_init_jobs()
    max_workers = min(get_settings().num_proc, max_proc) if max_proc > 0 else get_settings().num_proc
    task_repeat = repeat if repeat else max_workers

    logging.info(f"STARTING WITH N_WORKERS: {max_workers}, REPEAT: {task_repeat}")
    with get_context("spawn").Pool(initializer=partial(init_worker_process, engine, builder),
                                   processes=max_workers) as pool:
        start_ts = time.time()
        result = pool.map(partial(start_task_pool, engine, func, **kwargs), range(task_repeat))
        print(result)
    total_runtime = time.time() - start_ts
    print(
        result)  # [[47.84888879, 48.020549, 47.360947229999994, 47.131018299999994, 47.39814992], [47.50742029, 47.88334506, 46.89231461, 47.57275019, 48.03460203]]
    logging.info("worker pool terminated")
    return total_runtime, mean(itertools.chain(*result))


# , settings: Settings = Depends(get_settings)
async def process_sqs_msg(msg):
    if not msg:
        return
    try:
        if 'TopicArn' in msg and 'Message' in msg:
            message = msg['Message']
        else:
            message = msg

        cmd = message['cmd']  # search,index,create
        cmd_args = None
        try:
            cmd_args = json.loads(message['cmd_args'])  # {'doc_nb': 1000}
        except:
            pass
        max_workers = message['max_workers']  # -1,2,3,...
        concurrency = message.get('concurrency', get_settings().proc_concurrency)  # how many coroutines
        engine = message.get('engine', 'es')  # es, vespa, ...
        profile = message['profile']  # '1'
        repeat = message.get('repeat', None)  # 1

        func = None
        # TODO: Automated discovery of profiles
        if engine == 'es':
            if profile == '1':
                index_name = 'index1'
                builder = await Data1Builder.new(index=index_name)
            elif profile == '2':
                index_name = 'index2'
                builder = await Data2Builder.new(index=index_name)
            elif profile == '3':
                index_name = 'index3'
                builder = await Data3Builder.new(index=index_name)
            else:
                return
        elif engine == 'vespa':
            index_name = 'ecom'
            builder = await VespaData1Builder.new(index=index_name, data_file='data/vespa/vectors.zip')
        else:
            return

        if cmd == 'build_data':
            data_url = message.get('data_url', None)  # blob://container_name/filename.ext
            if data_url:
                # FIXME: hacky
                import regex as re
                result = re.search(r"(.*):\/\/(.*)\/(.*)", data_url)
                # provider = result.group(0) only supports azure blob storage for now
                container = result.group(1)
                filename = result.group(2)
                connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
                blob_service_client = BlobServiceClient.from_connection_string(connect_str)
                blob_client = blob_service_client.get_blob_client(container=container, blob=filename)

                download_file_path = f'data/vespa/{filename}'
                with open(download_file_path, "wb") as download_file:
                    download_file.write(blob_client.download_blob().readall())

            func = builder.build_data_repo
        elif cmd == 'create_index':
            func = builder.create_index
            concurrency = 1
        elif cmd == 'index_docs':
            func = builder.index_docs
        elif cmd == 'search':
            func = builder.run_queries
            builder.add_init_job(builder.load_embeddings)
            builder.add_init_job(partial(builder.load_queries, **cmd_args))
        total_runtime, mean_latency = await start_processors(engine, max_workers, repeat, builder, func,
                                                             concurrency=concurrency, **cmd_args)
        result = {
            "mean_latency": mean_latency,
        }
        if 'query_nb' in cmd_args:
            total_queries = min(max_workers, repeat) * concurrency * cmd_args['query_nb']
            result['total_queries'] = total_queries
            result['total_runtime'] = total_runtime
            result['queries_per_second'] = total_queries / (total_runtime)

        response = result, 200
    except Exception as ex:
        logging.exception('Error processing message: %s' % ex)
        response = ex, 500

    return response


@app.post("/cmd")
async def cmd(request: Request):
    # "cmd_args": "{\"query_nb\": 100, \"full_text_search\": 1, \"keyword_search\": 1, \"filter_search\": 1, \"nested_search\": 1, \"geopoint_search\": 1}",
    body = await request.json()
    if body is None:
        # Expect application/json request
        response = "", 415
    else:
        response = await process_sqs_msg(body)

    return response


async def poll_sqs_queue():
    # Create SQS client
    sqs = aioboto3.client('sqs')

    queue_url = get_settings().sqs_queue_url

    while True:
        # Receive message from SQS queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=400_000,
            WaitTimeSeconds=10
        )

        message = response['Messages'][0]

        receipt_handle = message['ReceiptHandle']
        await process_sqs_msg(message)
        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        logging.info('Received and deleted message: %s' % message)


if __name__ == "__main__":
    poll_thread = threading.Thread(target=poll_sqs_queue)
    logging.info(f"READING FROM QUEUE: {get_settings().sqs_queue_url}")
    poll_thread.start()

    uvicorn.run(app, host="0.0.0.0", port=5000)
