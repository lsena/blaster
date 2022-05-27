import logging
import os
import shutil
import time
from statistics import mean

import orjson

from builders.data_service import DataService, BenchmarkResult
from db.vespa_service import VespaService
from utils import get_settings


class VespaDataService(DataService):
    async def create_index(self, slot, subslot, total_subslots, conn):
        await VespaService.open_connection(conn)
        dest_path = f'{get_settings().tmp_data_folder}/application'
        ext = 'zip'
        shutil.make_archive(
            base_name=f'{dest_path}',
            format=ext,
            root_dir=self.mapping_path,
            base_dir='.'
        )
        resp = await VespaService.create_index(conn, self.index, f'{dest_path}.{ext}')
        print(resp.status_code, resp.text)
        os.remove(f'{dest_path}.{ext}')
        return BenchmarkResult()
        # await VespaService.close_connection(conn)

    async def index_docs(self, slot, subslot, total_subslots, conn):
        # results = []
        query_latency = []
        files = next(os.walk(f'{get_settings().tmp_data_folder}/vespa/docs/{slot}/'), (None, None, []))[
            2]  # [] if no file
        # TODO: this can break when we have very few files
        chunks = [files[i:i + total_subslots] for i in range(0, len(files), total_subslots)]
        if subslot >= len(chunks):
            logging.warning("subslot greater than number of chunks")
            return []
        await VespaService.open_connection(conn)
        ts = time.time_ns()
        for file in chunks[subslot]:
            buffer = []
            async for line in self.read_from_file(f'{get_settings().tmp_data_folder}/vespa/docs/{slot}/{file}'):
                buffer.append(orjson.loads(line))
                if len(buffer) > 1000:
                    await VespaService.bulk(conn, self.index, buffer)
                    buffer = []
            if buffer:
                await VespaService.bulk(conn, self.index, buffer)

            # actions = await self.read_file(f'data/vespa/docs/{slot}/{file}')

            # await VespaService.bulk(conn, self.index, orjson.loads(actions))
            query_latency.append(time.time_ns() - ts)
        # await VespaService.close_connection(conn)
        return BenchmarkResult(mean(query_latency) / 1_000_000, 0)

    async def build_query(self, **query_opts):
        raise NotImplementedError

    async def load_queries(self, query_nb, **query_opts):
        self.queries = []
        for _ in range(query_nb):
            self.queries.append(await self.build_query(**query_opts))

    async def run_queries(self, slot, subslot, total_subslots, conn, **query_opts):
        query_latency = []
        query_latency_from_server = []
        await VespaService.open_connection(conn)
        for query in self.queries:
            ts = time.time_ns()
            result = await VespaService.send_query(conn, index=self.index, body=query)
            query_latency.append(time.time_ns() - ts)
            query_latency_from_server.append(result.json['timing']['querytime'] * 1000)
        # return average (is ms) of all queries for each asyncio task
        return BenchmarkResult(mean(query_latency) / 1_000_000, mean(query_latency_from_server))

    async def get_recall(self, slot, subslot, total_subslots, conn):
        await self.load_embeddings()
        await VespaService.open_connection(conn)
        results = []
        query_latency = []
        query_latency_from_server = []
        for _ in range(500):
            ts = time.time_ns()
            query = await self.build_query(approximate=False)
            results_exact = await VespaService.send_query(conn, index=self.index, body=query)
            lst_ids_exact = [hit['id'] for hit in results_exact.hits]
            query['yql'] = query['yql'].replace("'approximate':true", "'approximate':false")
            results_approx = await VespaService.send_query(conn, index=self.index, body=query)
            query_latency.append(time.time_ns() - ts)
            query_latency_from_server.append(results_approx.json['timing']['querytime'] * 1000)

            lst_ids_approx = [hit['id'] for hit in results_approx.hits]
            true_positives = sum(x == y for x, y in zip(lst_ids_exact, lst_ids_approx))
            false_negatives = sum(x != y for x, y in zip(lst_ids_exact, lst_ids_approx))
            recall = true_positives / (true_positives + false_negatives)
            results.append(recall)
        return BenchmarkResult(mean(query_latency) / 1_000_000, mean(query_latency_from_server), response={'recall': mean(results)})
