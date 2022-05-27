import os
import random
import shutil
import time
import zipfile
from contextlib import AsyncExitStack

import aiofiles
import orjson

from builders.vespa.vespa_data_service import VespaDataService
from utils import get_settings


class VespaData1Builder(VespaDataService):
    mapping_path = f'{get_settings().static_data_folder}/vespa/schema/data1/application'

    async def generate_docs(self, idx):
        if self.data_file.endswith('.zip'):
            with zipfile.ZipFile(self.data_file, 'r') as zip_ref:
                zip_ref.extractall(f'{get_settings().tmp_data_folder}')

        total_slots = 4
        idx = 0

        for slot in range(total_slots):
            dir_path = f'{get_settings().tmp_data_folder}/vespa/docs/{slot}'
            try:
                shutil.rmtree(dir_path)
            except:
                pass
            os.makedirs(dir_path, exist_ok=True)

        async with AsyncExitStack() as stack:
            file_handlers = [await stack.enter_async_context(aiofiles.open(f'{get_settings().tmp_data_folder}/vespa/docs/{slot}/docs', mode='w'))
                             for slot in range(total_slots)]

            async with aiofiles.open(f'{get_settings().tmp_data_folder}/tmp/vector_mappings_main.json', mode='r') as f:

                # for slot in range(total_slots):
                #     await file_handlers[slot].write('[')
                # not_first_line = [False] * total_slots
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
                    # if not_first_line[current_slot]:
                    #     tmpstr = f",{orjson.dumps(tmp).decode('utf-8')}"
                    # else:
                    #     tmpstr = orjson.dumps(tmp).decode('utf-8')
                    #     not_first_line[current_slot] = True
                    await file_handlers[current_slot].write(orjson.dumps(tmp).decode('utf-8'))
                    await file_handlers[current_slot].write('\n')
                    idx += 1

                # for slot in range(total_slots):
                #     await file_handlers[slot].write(']')

    async def build_data_repo(self, slot, subslot, total_subslots, es, doc_nb):
        # FIXME: this is just a hack to break the file into slots for multiprocessing queries
        if slot and int(slot) != 0:
            return

        ts = time.time_ns()
        await self.generate_docs(None)
        return (time.time_ns() - ts) / 1_000_000

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
        approximate = 'true' if 'approximate' in query_opts else 'false'
        yql = f"select id from sources {self.index} where ([{{'approximate':{approximate}, 'targetHits': {page_size} }}]nearestNeighbor(embedding, query_embedding)) {qfilter};"
        query = {
            "yql": yql,
            "hits": page_size,
            "presentation.timing": True,
            'presentation.summary': 'id',
            "ranking.features.query(query_embedding)": embedding,
            "ranking.profile": "semantic",
            "timeout": 2000,
            "ranking.softtimeout.enable": False
        }

        return query
