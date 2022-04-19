import json
import logging
import os
import time

from builders.data_service import DataService
from db.elasticsearch_service import ElasticsearchService


class ElasticsearchDataService(DataService):

    async def create_index(self, slot, subslot, total_subslots, conn):
        await ElasticsearchService.create_index(conn, self.index, await self.read_file(self.mapping_path))

    async def index_docs(self, slot, subslot, total_subslots, conn):
        results = []
        files = next(os.walk(f'data/es/docs/{slot}/'), (None, None, []))[2]  # [] if no file
        # TODO: this can break when we have very few files
        chunks = [files[i:i + total_subslots] for i in range(0, len(files), total_subslots)]
        if subslot >= len(chunks):
            logging.warning("subslot greater than number of chunks")
            return []
        for file in chunks[subslot]:
            actions = await self.read_file(f'data/es/docs/{slot}/{file}')
            results.append(await ElasticsearchService.bulk(conn, json.loads(actions)))
        return results

    async def run_queries(self, slot, subslot, total_subslots, conn, query_nb, **query_opts):
        query_latency = []
        for _ in range(query_nb):
            body = await self.build_query(**query_opts)
            ts = time.time_ns()
            await ElasticsearchService.send_query(conn, index=self.index, body=body)
            query_latency.append(time.time_ns() - ts)
        return (sum(query_latency) / len(query_latency)) / 1_000_000
