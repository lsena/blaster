import logging
import requests

from vespa.application import Vespa, VespaAsync

from utils import get_settings

vespa_app = None


def get_connection(hosts='http://vespa:8080') -> VespaAsync:
    global vespa_app
    if not vespa_app:
        cert = f'{get_settings().static_data_folder}/vespa/cert/client.pem'
        key = f'{get_settings().static_data_folder}/vespa/cert/client.key'
        # vespa_app = Vespa(url=hosts,cert=cert, key=key).asyncio(connections=120, total_timeout=50)
        vespa_app = Vespa(url=hosts).asyncio(connections=200, total_timeout=50)
    return vespa_app


class VespaService():

    @classmethod
    async def open_connection(cls, conn: VespaAsync):
        await conn._open_aiohttp_session()

    @classmethod
    async def send_query(cls, conn: VespaAsync, index, body, **kwargs):
        result = await conn.query(
            body=body
        )
        if result.status_code > 300:
            logging.error(result.json)
        return result

    @classmethod
    async def change_settings(cls, es, index, **kwargs):
        pass

    @classmethod
    async def create_index(cls, conn: VespaAsync, index, mapping):
        with open(mapping, 'rb') as f:
            headers = {"Content-Type": "application/zip"}
            url = f'{get_settings().cluster_settings.connection_string}/application/v2/tenant/default/prepareandactivate'
            url = url.replace('8080', '19071') # FIXME
            data = f.read()
            r = requests.post(
                url=url,
                headers=headers,
                data=data,
                verify=False)
            return r

    @classmethod
    async def bulk(cls, conn: VespaAsync, schema, actions):
        results = await conn.feed_batch(schema=schema, batch=actions)
        return results

    @classmethod
    async def close_connection(cls, client: VespaAsync):
        await client._close_aiohttp_session()
