from elasticsearch import AsyncElasticsearch
from elasticsearch._async import helpers

es = None
def get_es_connection(hosts='es'):
    global es
    if not es:
        es = AsyncElasticsearch(hosts, maxsize=10, http_compress=True)
    return es


class ElasticsearchService():

    # @classmethod
    # async def query(cls, query_lst, index, batch_mode):
    #     if batch_mode == 'msearch':
    #         await cls.es.msearch(body=[
    #             {"index": "tvshows", "type": "tvshows"},
    #             {"query": {"match_all": {}}},
    #             {"index": "tvshows", "type": "tvseries"},
    #             {"query": {"match_all": {}}},
    #         ])
    #     elif batch_mode == ''

    @classmethod
    async def send_query(cls, es, index, body, **kwargs):
        resp = await es.search(
            index=index,
            body=body,
            **kwargs
        )
        return resp

    @classmethod
    async def change_settings(cls, es, index, **kwargs):
        # refresh_interval: None=default, -1, '1s'
        # import requests
        # requests.put(f'{ELASTIC_URL}/{elastic_index}/_settings',
        #              data=json.dumps({'number_of_replicas': 2, 'refresh_interval': '1s'}),
        #              headers={'Content-type': 'application/json', 'Accept': 'text/plain'})
        resp = await es.indices.put_settings(index=index, body={"refresh_interval": -1})
        return resp

    @classmethod
    async def create_index(cls, es, index, mapping):
        try:
            await es.indices.delete(index=index)
        except Exception:
            # TODO: index does not exist error. need to check type
            pass
        return await es.indices.create(index=index, body=mapping)

    @classmethod
    async def bulk(cls, es, actions):
        return await helpers.async_bulk(es, actions, request_timeout=300)

    @classmethod
    async def close_es_connection(cls, es):
        await es.close()
