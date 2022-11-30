import base64
import json
import os
import random
import time
import uuid

from pyroaring import BitMap

from builders.es.es_data_service import ElasticsearchDataService
from utils import get_settings


class ElasticsearchData2Builder(ElasticsearchDataService):
    mapping_path = f'{get_settings().static_data_folder}/es/mapping_2.json'

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
        dir_path = f'{get_settings().tmp_data_folder}/es/docs/{slot}'
        os.makedirs(dir_path, exist_ok=True)
        await self.write_file(f'{dir_path}/{file_name}', actions)

    async def build_query(self, **query_opts):
        field = 'filter_id'
        operation = ''
        terms_array = [random.randint(1, 2000000000) for _ in range(10000)]
        query = {
            "query": {
                "bool": {
                    "filter": {
                    }
                }
            }
        }
        if "bitmap" in query_opts:
            terms = base64.b64encode(BitMap.serialize(BitMap(terms_array))).decode('utf-8')
            query["query"]["bool"]["filter"]["script"] = {
                "script": {
                    "source": "fast_filter",
                    "lang": "fast_filter",
                    "params": {
                        "field": f"{field}",
                        "operation": f"{operation}",
                        "terms": f"{terms}"
                    }
                }
            }
        else:  # test https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-set-query.html
            query["query"]["bool"]["filter"]["terms"] = {
                field: terms_array
            }
        return query
