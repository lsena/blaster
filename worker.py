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
from builders.es.data5 import ElasticsearchData5Builder
from builders.es.data6 import ElasticsearchData6Builder
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
        cmd_args = {}
        try:
            cmd_args = json.loads(message['cmd_args'])  # {\"doc_nb\": 1000}
        except:
            pass
        max_workers = message['max_workers']  # -1,2,3,...
        max_workers = min(get_settings().num_proc, max_workers) if max_workers > 0 else get_settings().num_proc
        concurrency = message.get('concurrency', get_settings().proc_concurrency)  # how many coroutines
        engine = message.get('engine', 'es')  # es, vespa, ...
        profile = message['profile']  # '1'
        # repeat = message.get('repeat', None)  # 1

        func = None
        profiles = {
            'es_1': ElasticsearchData1Builder.new(index='index1'),
            'es_2': ElasticsearchData2Builder.new(index='index2'),
            'es_3': ElasticsearchData3Builder.new(index='index3'),
            'es_4': ElasticsearchData4Builder.new(index='index4',
                                                  data_file=f'{get_settings().tmp_data_folder}/vectors.zip'),
            'es_5': ElasticsearchData5Builder.new(index='index5'),
            'es_6': ElasticsearchData6Builder.new(index='index6'),
            'vespa_1': VespaData1Builder.new(index='ecom', data_file=f'{get_settings().tmp_data_folder}/vectors.zip'),
        }
        builder = profiles.get(f'{engine}_{profile}', None)
        if not builder:
            return {
                "invalid request"
            }
        else:
            builder = await builder

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
            func = partial(builder.index_docs, **cmd_args)
        elif cmd == 'search':
            func = builder.run_queries
            builder.add_init_job(builder.load_embeddings)
            builder.add_init_job(partial(builder.load_queries, **cmd_args))
        elif cmd == 'get_recall':
            func = builder.get_recall
        bench_result = await start_processors(engine, max_workers, builder, func,
                                              concurrency=concurrency, **cmd_args)
        result = {
            "mean_duration_server": bench_result.mean_duration_server,
            "mean_duration_client": bench_result.mean_duration_client,
        }
        if 'query_nb' in cmd_args:
            # total_queries = min(max_workers, repeat) * concurrency * cmd_args['query_nb']
            total_queries = max_workers * concurrency * cmd_args['query_nb']
            result['total_queries'] = total_queries
            result['total_runtime'] = bench_result.total_duration
            result['queries_per_second'] = total_queries / (bench_result.total_duration)
        if bench_result.response:
            result.update(bench_result.response)
        print(result)
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
        logging.info("Received request from remote client, processing...")
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
