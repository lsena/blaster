import json
import logging
import threading
from functools import partial

import uvicorn
from fastapi import FastAPI, Request

from builders.es.data1 import ElasticsearchData1Builder
from builders.es.data2 import ElasticsearchData2Builder
from builders.es.data3 import ElasticsearchData3Builder
from builders.es.data4 import ElasticsearchData4Builder
from builders.vespa.data1 import VespaData1Builder
from processor import start_processors
from utils import get_settings, download_remote_data, poll_sqs_queue

app = FastAPI()


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
                builder = await ElasticsearchData1Builder.new(index=index_name)
            elif profile == '2':
                index_name = 'index2'
                builder = await ElasticsearchData2Builder.new(index=index_name)
            elif profile == '3':
                index_name = 'index3'
                builder = await ElasticsearchData3Builder.new(index=index_name)
            elif profile == '4':
                index_name = 'index4'
                builder = await ElasticsearchData4Builder.new(index=index_name, data_file='data/vespa/vectors.zip')
            else:
                return
        elif engine == 'vespa':
            index_name = 'ecom'
            builder = await VespaData1Builder.new(index=index_name, data_file='data/vespa/vectors.zip')
        else:
            return

        if cmd == 'build_data':
            data_url = message.get('data_url', None)  # blob://container_name/filename.ext
            queries_url = message.get('queries_url', None)  # blob://container_name/filename.ext
            if data_url:
                download_remote_data(data_url)
            if queries_url:
                download_remote_data(queries_url)

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


async def red_msgs():
    message = await poll_sqs_queue()
    while message:
        await process_sqs_msg(message)
        message = await poll_sqs_queue()
        logging.info('Received and deleted message: %s' % message)


if __name__ == "__main__":
    poll_thread = threading.Thread(target=poll_sqs_queue)
    logging.info(f"READING FROM QUEUE: {get_settings().sqs_queue_url}")
    poll_thread.start()

    uvicorn.run(app, host="0.0.0.0", port=5000)
