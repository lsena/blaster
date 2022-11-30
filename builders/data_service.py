import base64
import os
import random
import time

import aiofiles
import orjson

from utils import get_settings


class BenchmarkResult:

    def __init__(self, mean_duration_client=0.0, mean_duration_server=0.0, total_duration=0.0, response=None):
        self.mean_duration_client = mean_duration_client
        self.mean_duration_server = mean_duration_server
        self.total_duration = total_duration
        self.response = response

    def __str__(self):
        return f'duration_client: {self.mean_duration_client}, duration_server: {self.mean_duration_server}'

    def __repr__(self):
        return f'BenchmarkResult("{self.mean_duration_client}, {self.mean_duration_server}")'


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
        if format == 'int':
            # TODO: return monotonically
            return int(f"1{str(time.time_ns())[8:-5]}{random.randint(0,999)}")
        else:
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
        seed_file = f'{get_settings().static_data_folder}/seed_en.txt'
        seed_file_words = await self.read_file(seed_file)
        self.seed_file_words_lst = seed_file_words.split('\n')

        seed_file = f'{get_settings().static_data_folder}/seed_fashion_en.txt'
        seed_file_words = await self.read_file(seed_file)
        self.seed_file_fashion_words_lst = seed_file_words.split('\n')

    async def get_rnd_txt(self, limit, format, repo_name=None):
        repo = self.seed_file_words_lst
        if repo_name == 'fashion':
            repo = self.seed_file_fashion_words_lst
        if format == 'string':
            result = ''
            for _ in range(limit):
                result = f'{result} {random.choice(repo)}'
            return result.strip()
        else:
            result = []
            for _ in range(limit):
                result.append(random.choice(repo))
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
        try:
            if not hasattr(self, 'query_embeddings_lst'):
                async with aiofiles.open(f'{get_settings().tmp_data_folder}/queries.json', mode='r') as f:
                    self.query_embeddings_lst = []
                    async for line in f:
                        self.query_embeddings_lst.append(orjson.loads(line))

        except Exception as ex:
            print('error loading embeddings file')

    async def load_queries(self, query_nb, **query_opts):
        self.queries = []
        for _ in range(query_nb):
            self.queries.append(await self.build_query(**query_opts))
