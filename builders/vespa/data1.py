import os
import random
import shutil
import zipfile
from contextlib import AsyncExitStack

import aiofiles
import orjson

from builders.vespa.vespa_data_service import VespaDataService


class VespaData1Builder(VespaDataService):
    mapping_path = 'data/vespa/mapping_1.json'

    # def __init__(self, index):
    #     super().__init__(index)
    #     query_embeddings_file = 'data/query_embeddings.txt'
    #     seed_file = await self.read_file(query_embeddings_file)
    #     self.query_embeddings_lst = seed_file.split('\n')

    def buf_count_newlines_gen(self, fname):
        def _make_gen(reader):
            while True:
                b = reader(2 ** 16)
                if not b: break
                yield b

        with open(fname, "rb") as f:
            count = sum(buf.count(b"\n") for buf in _make_gen(f.raw.read))
        return count

    async def generate_docs(self, idx):
        if self.data_file.endswith('.zip'):
            with zipfile.ZipFile(self.data_file, 'r') as zip_ref:
                zip_ref.extractall('data/vespa')

        total_slots = 4
        idx = 0

        for slot in range(total_slots):
            dir_path = f'data/vespa/docs/{slot}'
            try:
                shutil.rmtree(dir_path)
            except:
                pass
            os.makedirs(dir_path, exist_ok=True)

        async with AsyncExitStack() as stack:
            file_handlers = [await stack.enter_async_context(aiofiles.open(f'data/vespa/docs/{slot}/docs', mode='w'))
                             for slot in range(total_slots)]

            async with aiofiles.open('data/vespa/tmp/vector_mappings_main.json', mode='r') as f:

                for slot in range(total_slots):
                    await file_handlers[slot].write('[')
                    # file_handlers[slot].flush()
                not_first_line = [False] * total_slots
                async for line in f:
                    tmp = {}
                    tmp['fields'] = orjson.loads(line)
                    tmp['fields']['stock_size_10'] = random.randint(0, 20)
                    tmp['id'] = tmp['fields']['id']
                    tmp['fields']['embedding'] = {
                        "values": tmp['fields']['embedding']
                    }
                    del tmp['fields']['id']
                    current_slot = idx % total_slots
                    if not_first_line[current_slot]:
                        tmpstr = f",{orjson.dumps(tmp).decode('utf-8')}"
                    else:
                        tmpstr = orjson.dumps(tmp).decode('utf-8')
                        not_first_line[current_slot] = True
                    await file_handlers[current_slot].write(tmpstr)
                    idx += 1

                for slot in range(total_slots):
                    await file_handlers[slot].write(']')

    async def build_data_repo(self, slot, subslot, total_subslots, es, doc_nb):
        # FIXME: this is just a hack to break the file into slots for multiprocessing queries
        if slot and int(slot) != 1:
            return
        await self.generate_docs(None)

    async def load_embeddings(self):
        # if self.data_file.endswith('.zip'):
        #     with zipfile.ZipFile(self.data_file, 'r') as zip_ref:
        #         zip_ref.extractall('data/vespa')
        if not hasattr(self, 'query_embeddings_lst'):
            async with aiofiles.open('data/vespa/tmp/vector_mappings_main.json', mode='r') as f:
                self.query_embeddings_lst = []
                async for line in f:
                    self.query_embeddings_lst.append(orjson.loads(line))

    async def build_query(self, **query_opts):
        # id, title, description, imUrl, brands [str], embedding [float]

        page_size = 10
        qfilter = ''
        if 'filter' in query_opts:
            qfilter = f'AND stock_size_10 >= {random.randint(0, 100)}'
        free_text_query = ''
        if 'sparse' in query_opts: #TODO
            free_text_str = ''
            free_text_query = " or ([{'targetHits': 10}]weakAnd(productDisplayName contains '" + free_text_str + "')) )"
        embedding = random.choice(self.query_embeddings_lst)['embedding']
        yql = f"select id from sources {self.index} where ([{{'targetHits': {page_size} }}]nearestNeighbor(embedding, query_embedding)) {qfilter};"
        query = {
            "yql": yql,
            "hits": page_size,
            "presentation.timing": True,
            "ranking.features.query(query_embedding)": str(embedding),
            "ranking.profile": "semantic"
        }

        return query
