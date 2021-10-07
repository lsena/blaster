import base64
import datetime
import json
import os
import random
import time
import uuid

import aiofiles

from elasticsearch_service import ElasticsearchService


class CreateDataService():
    colors = ['amber', 'blue', 'brown', 'gray', 'green', 'hazel', 'red']
    mapping_path = None

    def __init__(self, index):
        self.index = index

    @classmethod
    async def new(cls, index):
        self = cls(index)
        await self.load_seed_text()
        return self

    def generate_random_base64(self, size):
        return base64.urlsafe_b64encode(os.urandom(size)).decode('utf-8').replace('=', '')

    async def generate_id(self, format='uuid4'):
        # TODO: lucene friendly formats
        ts = int(time.time_ns() / 1000)
        return f"{str(ts)}{self.generate_random_base64(3)}"

    async def read_file(self, file_path):
        async with aiofiles.open(file_path, mode='r') as f:
            file_contents = await f.read()
        return file_contents

    async def write_file(self, file_path, contents):
        async with aiofiles.open(file_path, mode='w') as f:
            file_contents = await f.write(contents)
        return file_contents

    async def load_seed_text(self):
        seed_file = 'data/seed_en.txt'
        seed_file_words = await self.read_file(seed_file)
        self.seed_file_words_lst = seed_file_words.split('\n')

    async def get_rnd_txt(self, limit, format):
        if format == 'string':
            result = ''
            for _ in range(limit):
                result = f'{result} {random.choice(self.seed_file_words_lst)}'
            return result.strip()
        else:
            result = []
            for _ in range(limit):
                result.append(random.choice(self.seed_file_words_lst))
        return result

    async def build_doc_file(self, doc_nb):
        raise NotImplementedError

    async def create_index(self, slot, es):
        await ElasticsearchService.create_index(es, self.index, await self.read_file(self.mapping_path))

    async def index_docs(self, slot, es):
        results = []
        files = next(os.walk(f'data/docs/{slot}/'), (None, None, []))[2]  # [] if no file
        # chuncks = [files[i:i + procs] for i in range(0, len(files), procs)]
        for file in files:
            actions = await self.read_file(f'data/docs/{slot}/{file}')
            results.append(await ElasticsearchService.bulk(es, json.loads(actions)))
        return results

    async def build_query(self, **query_opts):
        raise NotImplementedError

    async def run_queries(self, slot, es, query_nb, **query_opts):
        query_latency = []
        for _ in range(query_nb):
            body = await self.build_query(**query_opts)
            ts = time.time_ns()
            await ElasticsearchService.send_query(es, index=self.index, body=body)
            query_latency.append(time.time_ns() - ts)
        return (sum(query_latency) / len(query_latency)) / 1_000_000


class Data1Builder(CreateDataService):
    mapping_path = 'data/mapping_1.json'

    async def generate_docs(self, idx):
        ts = time.time()

        # docs = []
        # for idx in range(doc_nb):
        now = datetime.datetime.now()
        doc_schema = {}
        # doc_schema['_id'] = self.generate_id()
        doc_schema['idx'] = idx
        doc_schema['guid'] = str(uuid.uuid4())
        doc_schema['isActive'] = True if random.randint(0, 9) > 5 else False
        doc_schema['price'] = round(random.uniform(1, 20000), 4)
        doc_schema['insert_time'] = now.strftime('%Y-%m-%dT%H:%M:%S')
        doc_schema['age'] = random.randint(1, 120)
        doc_schema['eyeColor'] = random.choice(self.colors)
        doc_schema['name'] = await self.get_rnd_txt(2, 'string')
        doc_schema['gender'] = await self.generate_id()
        doc_schema['company'] = await self.get_rnd_txt(2, 'string')
        doc_schema['email'] = f"{await self.get_rnd_txt(1, 'string')}@{await self.get_rnd_txt(1, 'string')}.com"
        doc_schema['phone'] = ''
        doc_schema['address'] = await self.get_rnd_txt(random.randint(3, 15), 'string')
        doc_schema['about'] = await self.get_rnd_txt(random.randint(3, 100), 'string')
        doc_schema['registered'] = datetime.date(random.randint(1990, 2020), random.randint(1, 12),
                                                 random.randint(1, 25)).strftime('%Y-%m-%d')
        doc_schema['location'] = {
            "lat": round(random.uniform(-90, 90), 2),
            "lon": round(random.uniform(-180, 180), 2)
        }
        doc_schema['review_scores'] = [random.randint(0, 10) for _ in range(random.randint(1, 30))]
        doc_schema['tags'] = await self.get_rnd_txt(random.randint(3, 20), 'list')
        doc_schema['friends_nested'] = [
            {"id": random.randint(1, 2000000000), "name": await self.get_rnd_txt(2, 'string')}
            for _ in range(10)
        ]
        doc_schema['greeting'] = await self.get_rnd_txt(random.randint(3, 10), 'string')
        action = {
            "_op_type": "update",  # TODO: benchmark VS normal index?
            "_index": self.index,
            "doc_as_upsert": True,
            "retry_on_conflict": 3,
            "_id": await self.generate_id(),
            "doc": doc_schema,
        }
        return action
        # docs.append(doc_schema)
        # print(time.time() - ts)

    async def build_data_repo(self, slot, es, doc_nb):
        print(f"BUILDING data repo slot:{slot} doc_nb:{doc_nb}")
        ts = time.time()
        actions = json.dumps([await self.generate_docs(idx) for idx in range(doc_nb)])
        file_name = await self.generate_id()
        # file_slot = hash(file_name) % get_settings()
        dir_path = f'data/docs/{slot}'
        os.makedirs(dir_path, exist_ok=True)
        print(f"WRITING data repo file:{file_name} slot:{slot} doc_nb:{doc_nb}")
        await self.write_file(f'{dir_path}/{file_name}', actions)

    async def build_query(self, **query_opts):
        query = {
            "query": {
                "bool": {
                    "must": [],
                    "should": [],
                    "filter": []
                }
            }
        }

        # match query
        if 'full_text_search' in query_opts:
            query['query']['bool']['must'].append({
                "match": {"name": await self.get_rnd_txt(1, 'string')}
            })
            query['query']['bool']['should'].append({
                "match": {"address": await self.get_rnd_txt(1, 'string')}
            })
            query['query']['bool']['should'].append({
                "match": {"about": await self.get_rnd_txt(1, 'string')}
            })

        # keyword search/filter?
        if 'keyword_search' in query_opts:
            query['query']['bool']['must'].append({
                "match": {"email": f"{await self.get_rnd_txt(1, 'string')}@{await self.get_rnd_txt(1, 'string')}.com"}
            })

        # filter query
        if 'filter_search' in query_opts:
            query['query']['bool']['filter'].append({
                "term": {"status": "published"}
            })
            query['query']['bool']['filter'].append({
                "range": {"registered": {"gte": datetime.date(random.randint(1990, 2020), random.randint(1, 12),
                                                              random.randint(1, 25)).strftime('%Y-%m-%d')}}
            })
        # nested query
        # if 'nested_search' in query_opts:
        #     query['query']['bool']['filter'].append({
        #         "geo_distance": {
        #             "distance": "200km",
        #             "pin.location": {
        #                 "latitude": round(random.uniform(-90, 90), 2),
        #                 "longitude": round(random.uniform(-180, 180), 2)
        #             }
        #         }
        #     })
        # geo_point query
        if 'geopoint_search' in query_opts:
            query['query']['bool']['filter'].append({
                "geo_distance": {
                    "distance": "200km",
                    "location": {
                        "lat": round(random.uniform(-90, 90), 2),
                        "long": round(random.uniform(-180, 180), 2)
                    }
                }
            })

        # sort by score (default)
        # sort by _doc (index order)
        # sort by float index
        # sort by float doc_values index
        # sort by nested value index
        # sort by nested value doc_values
        # sort by geo distance

        return query
