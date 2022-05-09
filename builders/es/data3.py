import base64
import json
import os
import random
import time
import uuid

import msgpack

from builders.es.es_data_service import ElasticsearchDataService


class ElasticsearchData3Builder(ElasticsearchDataService):
    mapping_path = 'data/es/mapping_3.json'

    async def generate_docs(self, idx):
        doc_schema = {}
        doc_schema['idx'] = idx
        doc_schema['guid'] = str(uuid.uuid4())
        doc_schema['filter_id'] = random.randint(1, 2000000000)
        doc_schema['important_field'] = await self.get_rnd_txt(2, 'string')

        action = {
            "_op_type": "update",
            "_index": self.index,
            "doc_as_upsert": True,
            "retry_on_conflict": 3,
            "_id": await self.generate_id(),
            "doc": doc_schema,
        }
        return action
        # docs.append(doc_schema)
        # print(time.time() - ts)

    async def build_data_repo(self, slot, subslot, total_subslots, conn, doc_nb):
        ts = time.time()
        actions = json.dumps([await self.generate_docs(idx) for idx in range(doc_nb)])
        file_name = await self.generate_id()
        # file_slot = hash(file_name) % get_settings()
        dir_path = f'data/es/docs/{slot}'
        os.makedirs(dir_path, exist_ok=True)
        await self.write_file(f'{dir_path}/{file_name}', actions)

    async def build_query(self, **query_opts):
        terms = base64.b64encode(msgpack.packb([random.randint(1, 2000000000) for _ in range(10000)])).decode("utf-8")
        query = {
            "query": {
                "function_score": {
                    "min_score": 0,
                    "query": {
                        "match_all": {}
                    },
                    "script_score": {
                        "script": {
                            "source": "fast_scorer",
                            "lang": "fast_scorer",
                            "params": {
                                "explicit_scores": False,
                                "field": "filter_id",
                                "operation": "include",
                                "terms": terms
                            }
                        }
                    }
                }
            }
        }
        return query
