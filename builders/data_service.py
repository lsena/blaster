import base64
import os
import random
import time

import aiofiles
import orjson


class DataService():
    colors = ['amber', 'blue', 'brown', 'gray', 'green', 'hazel', 'red']
    mapping_path = None

    def __init__(self, index, data_file: str = None):
        self.index = index
        self.data_file = data_file
        self.init_jobs = []
        self.queries = []

    @classmethod
    async def new(cls, index, data_file: str = None):
        self = cls(index, data_file)
        await self.load_seed_text()
        return self

    async def run_init_jobs(self):
        for job_func in self.init_jobs:
            await job_func()

    def add_init_job(self, job_func):
        self.init_jobs.append(job_func)

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

    async def read_from_file(self, file_path):
        async with aiofiles.open(file_path, mode='r') as f:
            async for line in f:
                yield line

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

    async def create_index(self, slot, subslot, total_subslots, conn):
        raise NotImplementedError

    async def index_docs(self, slot, subslot, total_subslots, conn):
        raise NotImplementedError

    async def build_query(self, **query_opts):
        raise NotImplementedError

    async def run_queries(self, slot, subslot, total_subslots, conn, **query_opts):
        raise NotImplementedError

    async def load_embeddings(self):
        if not hasattr(self, 'query_embeddings_lst'):
            async with aiofiles.open('data/queries.json', mode='r') as f:
                self.query_embeddings_lst = []
                async for line in f:
                    self.query_embeddings_lst.append(orjson.loads(line))
