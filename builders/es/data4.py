import os
import random
import shutil
import time
import zipfile
from contextlib import AsyncExitStack

import aiofiles
import orjson

from builders.es.es_data_service import ElasticsearchDataService


class ElasticsearchData4Builder(ElasticsearchDataService):
    mapping_path = 'data/es/mapping_4.json'

    async def generate_docs(self, idx):
        if self.data_file.endswith('.zip'):
            with zipfile.ZipFile(self.data_file, 'r') as zip_ref:
                zip_ref.extractall('data/es')

        total_slots = 4
        idx = 0

        for slot in range(total_slots):
            dir_path = f'data/es/docs/{slot}'
            try:
                shutil.rmtree(dir_path)
            except:
                pass
            os.makedirs(dir_path, exist_ok=True)

        async with AsyncExitStack() as stack:
            file_handlers = [await stack.enter_async_context(aiofiles.open(f'data/es/docs/{slot}/docs', mode='w'))
                             for slot in range(total_slots)]

            async with aiofiles.open('data/es/tmp/vector_mappings_main.json', mode='r') as f:
                # for slot in range(total_slots):
                #     await file_handlers[slot].write('[')
                # not_first_line = [False] * total_slots
                async for line in f:
                    tmp = {}

                    tmp['idx'] = idx
                    tmp.update(orjson.loads(line))
                    tmp['stock_size_10'] = random.randint(0, 20)

                    action = {
                        "_op_type": "update",
                        "_index": self.index,
                        "doc_as_upsert": True,
                        "retry_on_conflict": 3,
                        "_id": await self.generate_id(),
                        "doc": tmp,
                    }
                    current_slot = idx % total_slots
                    await file_handlers[current_slot].write(orjson.dumps(action).decode('utf-8'))
                    await file_handlers[current_slot].write('\n')
                    idx += 1

    async def build_data_repo(self, slot, subslot, total_subslots, es, doc_nb):
        # FIXME: this is just a hack to break the file into slots for multiprocessing queries
        if slot and int(slot) != 0:
            return

        ts = time.time_ns()
        await self.generate_docs(None)
        return (time.time_ns() - ts) / 1_000_000

    async def build_query(self, **query_opts):
        embedding = random.choice(self.query_embeddings_lst)['embedding']
        query = {
            "size": 10,
            "_source": False,
            "query": {
                "script_score": {
                    "query": {
                        "match_all": {}
                    },
                    # "query": {
                    #     "bool": {
                    #         "filter": {
                    #             "term": {
                    #                 "status": "published"
                    #             }
                    #         }
                    #     }
                    # },
                    "script": {
                        "source": "l2norm(params.query_vector, 'embedding') + 1.0",
                        "params": {
                            "query_vector": embedding
                        }
                    }
                }
            }
        }
        return query
