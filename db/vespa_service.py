import logging

from vespa.application import Vespa, VespaAsync

vespa_app = None


def get_connection(hosts='http://vespa:8080') -> VespaAsync:
    global vespa_app
    if not vespa_app:
        cert = 'data/vespa/cert/client.pem'
        key = 'data/vespa/cert/client.key'
        # vespa_app = Vespa(url=hosts,cert=cert, key=key).asyncio(connections=120, total_timeout=50)
        vespa_app = Vespa(url=hosts).asyncio(connections=120, total_timeout=50)
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
    async def create_index(cls, es, index, mapping):
        try:
            await es.indices.delete(index=index)
        except Exception:
            # TODO: index does not exist error. need to check type
            pass
        return await es.indices.create(index=index, body=mapping)

    @classmethod
    async def bulk(cls, conn: VespaAsync, schema, actions):
        results = await conn.feed_batch(schema=schema, batch=actions)
        return results

    @classmethod
    async def close_connection(cls, client: VespaAsync):
        await client._close_aiohttp_session()
