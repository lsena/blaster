import json
import logging
import os
import time
from statistics import mean

import orjson

from builders.data_service import DataService, BenchmarkResult
from db.elasticsearch_service import ElasticsearchService
from utils import get_settings


class ElasticsearchDataService(DataService):

    async def create_index(self, slot, subslot, total_subslots, conn):
        await ElasticsearchService.create_index(conn, self.index, await self.read_file(self.mapping_path))
        return BenchmarkResult()

    async def index_docs(self, slot, subslot, total_subslots, conn):
        # results = []
        query_latency = []
        files = next(os.walk(f'{get_settings().tmp_data_folder}/es/docs/{slot}/'), (None, None, []))[2]  # [] if no file
        # TODO: this can break when we have very few files
        chunks = [files[i:i + total_subslots] for i in range(0, len(files), total_subslots)]
        if subslot >= len(chunks):
            logging.warning("subslot greater than number of chunks")
            return []
        ts = time.time_ns()
        for file in chunks[subslot]:
            buffer = []
            async for line in self.read_from_file(f'{get_settings().tmp_data_folder}/es/docs/{slot}/{file}'):
                buffer += orjson.loads(line)
                if len(buffer) > 1000:
                    await ElasticsearchService.bulk(conn, buffer)
                    buffer = []
            if buffer:
                result = await ElasticsearchService.bulk(conn, buffer)

            query_latency.append(time.time_ns() - ts)
        return BenchmarkResult(mean(query_latency) / 1_000_000, 0)

    async def build_query(self, **query_opts):
        raise NotImplementedError

    async def run_queries(self, slot, subslot, total_subslots, conn, query_nb, **query_opts):
        query_latency = [0] * len(self.queries)
        query_latency_from_server = [0] * len(self.queries)
        for idx, query in enumerate(self.queries):
            ts = time.time_ns()
            result = await ElasticsearchService.send_query(conn, index=self.index, body=query)
            query_latency[idx] = time.time_ns() - ts
            query_latency_from_server[idx] = result['took']
        return BenchmarkResult(mean(query_latency) / 1_000_000, mean(query_latency_from_server))
