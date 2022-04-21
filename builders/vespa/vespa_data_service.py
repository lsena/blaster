import logging
import os
import time
from statistics import mean

import orjson

from builders.data_service import DataService
from db.vespa_service import VespaService


class VespaDataService(DataService):
    async def create_index(self, slot, subslot, total_subslots, conn):
        await VespaService.open_connection(conn)
        await VespaService.create_index(conn, self.index, await self.read_file(self.mapping_path))
        # await VespaService.close_connection(conn)

    async def index_docs(self, slot, subslot, total_subslots, conn):
        # results = []
        query_latency = []
        files = next(os.walk(f'data/vespa/docs/{slot}/'), (None, None, []))[2]  # [] if no file
        # TODO: this can break when we have very few files
        chunks = [files[i:i + total_subslots] for i in range(0, len(files), total_subslots)]
        if subslot >= len(chunks):
            logging.warning("subslot greater than number of chunks")
            return []
        await VespaService.open_connection(conn)
        for file in chunks[subslot]:
            actions = await self.read_file(f'data/vespa/docs/{slot}/{file}')
            ts = time.time_ns()
            await VespaService.bulk(conn, self.index, orjson.loads(actions))
            query_latency.append(time.time_ns() - ts)
        # await VespaService.close_connection(conn)
        return mean(query_latency) / 1_000_000

    async def build_query(self, **query_opts):
        raise NotImplementedError

    async def load_embeddings(self):
        return

    async def load_queries(self, query_nb, **query_opts):
        self.queries = []
        for _ in range(query_nb):
            self.queries.append(await self.build_query(**query_opts))

    async def run_queries(self, slot, subslot, total_subslots, conn, **query_opts):
        query_latency = []
        await VespaService.open_connection(conn)
        for query in self.queries:
            ts = time.time_ns()
            await VespaService.send_query(conn, index=self.index, body=query)
            query_latency.append(time.time_ns() - ts)
        # return average (is ms) of all queries for each asyncio task
        return mean(query_latency) / 1_000_000
