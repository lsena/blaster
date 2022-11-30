import datetime
import json
import os
import random
import time
import uuid

from builders.es.es_data_service import ElasticsearchDataService
from utils import get_settings


class ElasticsearchData5Builder(ElasticsearchDataService):
    mapping_path = f'{get_settings().static_data_folder}/es/mapping_1.json'

    async def generate_docs(self, idx):
        ts = time.time()

        # docs = []
        # for idx in range(doc_nb):
        now = datetime.datetime.now()
        doc_schema = {}
        # doc_schema['_id'] = self.generate_id()
        doc_schema['idx'] = idx
        doc_schema['guid'] = str(uuid.uuid4())
        doc_schema['isActive'] = True if random.randint(0, 9) > 5 else False
        doc_schema['price'] = round(random.uniform(1, 20000), 4)
        doc_schema['insert_time'] = now.strftime('%Y-%m-%dT%H:%M:%S')
        doc_schema['age'] = random.randint(1, 120)
        doc_schema['eyeColor'] = random.choice(self.colors)
        doc_schema['name'] = await self.get_rnd_txt(2, 'string')
        doc_schema['gender'] = await self.generate_id()
        doc_schema['company'] = await self.get_rnd_txt(2, 'string')
        doc_schema['email'] = f"{await self.get_rnd_txt(1, 'string')}@{await self.get_rnd_txt(1, 'string')}.com"
        doc_schema['phone'] = ''
        doc_schema['address'] = await self.get_rnd_txt(random.randint(3, 15), 'string')
        doc_schema['about'] = await self.get_rnd_txt(random.randint(3, 100), 'string')
        doc_schema['registered'] = datetime.date(random.randint(1990, 2020), random.randint(1, 12),
                                                 random.randint(1, 25)).strftime('%Y-%m-%d')
        doc_schema['location'] = {
            "lat": round(random.uniform(-90, 90), 2),
            "lon": round(random.uniform(-180, 180), 2)
        }
        doc_schema['review_scores'] = [random.randint(0, 10) for _ in range(random.randint(1, 30))]
        doc_schema['tags'] = await self.get_rnd_txt(random.randint(3, 20), 'list')
        doc_schema['friends_nested'] = [
            {"id": random.randint(1, 2000000000), "name": await self.get_rnd_txt(2, 'string')}
            for _ in range(10)
        ]
        doc_schema['greeting'] = await self.get_rnd_txt(random.randint(3, 10), 'string')

        boost_score_a = random.uniform(0, 1)
        boost_score_b = random.uniform(0, 1)
        doc_schema['booster1a'] = boost_score_a
        doc_schema['booster1b'] = boost_score_b
        doc_schema['booster2a'] = boost_score_a
        doc_schema['booster2b'] = boost_score_b

        action = {
            "_op_type": "update",  # TODO: benchmark VS normal index?
            "_index": self.index,
            "doc_as_upsert": True,
            "retry_on_conflict": 3,
            "_id": await self.generate_id(),
            "doc": doc_schema,
        }
        return action
        # docs.append(doc_schema)
        # print(time.time() - ts)

    async def build_data_repo(self, slot, subslot, total_subslots, conn, doc_nb):
        ts = time.time()
        actions = json.dumps([await self.generate_docs(idx) for idx in range(doc_nb)])
        file_name = await self.generate_id()
        # file_slot = hash(file_name) % get_settings()
        dir_path = f'{get_settings().tmp_data_folder}/es/docs/{slot}'
        os.makedirs(dir_path, exist_ok=True)
        await self.write_file(f'{dir_path}/{file_name}', actions)

    async def build_query(self, **query_opts):
        query = {
            "query": {
                "bool": {
                    "must": [],
                    "should": [],
                    "filter": []
                }
            }
        }

        # match query
        if 'full_text_search' in query_opts:
            query['query']['bool']['must'].append({
                "match": {"name": await self.get_rnd_txt(1, 'string')}
            })
            query['query']['bool']['should'].append({
                "match": {"address": await self.get_rnd_txt(1, 'string')}
            })
            query['query']['bool']['should'].append({
                "match": {"about": await self.get_rnd_txt(1, 'string')}
            })

        # keyword search/filter?
        if 'keyword_search' in query_opts:
            query['query']['bool']['must'].append({
                "match": {"email": f"{await self.get_rnd_txt(1, 'string')}@{await self.get_rnd_txt(1, 'string')}.com"}
            })

        # filter query
        if 'filter_search' in query_opts:
            query['query']['bool']['filter'].append({
                "term": {"status": "published"}
            })
            query['query']['bool']['filter'].append({
                "range": {"registered": {"gte": datetime.date(random.randint(1990, 2020), random.randint(1, 12),
                                                              random.randint(1, 25)).strftime('%Y-%m-%d')}}
            })
        # nested query
        # if 'nested_search' in query_opts:
        #     query['query']['bool']['filter'].append({
        #         "geo_distance": {
        #             "distance": "200km",
        #             "pin.location": {
        #                 "latitude": round(random.uniform(-90, 90), 2),
        #                 "longitude": round(random.uniform(-180, 180), 2)
        #             }
        #         }
        #     })
        # geo_point query
        if 'geopoint_search' in query_opts:
            query['query']['bool']['filter'].append({
                "geo_distance": {
                    "distance": "200km",
                    "location": {
                        "lat": round(random.uniform(-90, 90), 2),
                        "long": round(random.uniform(-180, 180), 2)
                    }
                }
            })

        # sort by score (default)
        # sort by _doc (index order)
        # sort by float index
        # sort by float doc_values index
        # sort by nested value index
        # sort by nested value doc_values
        # sort by geo distance
        if 'sort_fields' in query_opts:
            sort_type = query_opts.get('sort_type', None)
            sort_fields = query_opts['sort_fields'].split(',')
            if sort_type == 'function_score':
                score_func = '+'.join(f"doc['{field_name}'].value" for field_name in sort_fields)
                query['query']['bool']['should'].append({
                    "function_score": {
                        "query": {
                            "match_all": {}
                        },
                        "script_score": {
                            "script": {
                                "source": score_func
                            }
                        }
                    }
                })
            elif sort_type == 'rank_feature':
                rank_features = []
                for idx, field_name in enumerate(reversed(sort_fields), 1):
                    rank_features.append({
                        "rank_feature": {
                            "field": field_name,
                            "boost": idx
                        }
                    })
                query['query']['bool']['should'].extend(rank_features)
            elif sort_type == 'field_value_factor':
                score_func = []
                for idx, field_name in enumerate(reversed(sort_fields), 1):
                    score_func.append({
                        "field_value_factor": {
                            "field": field_name,
                        }
                    })
                score_func.append({
                    "multi-index-rescore-v4": {
                        "field": "idx",
                        "context": "test",
                        "index": "test"
                    }
                })
                query['query']['bool']['should'].append({
                    "function_score": {
                        "query": {
                            "match_all": {}
                        },
                        "functions": score_func
                    }
                })
            else:  # default if sort
                query['sort'] = [
                    "_score",
                    *sort_fields
                ]
        if 'rescore' in query_opts:
            rescore_window = query_opts.get('rescore', None)
            query['rescore'] = {
                "window_size": rescore_window,
                "live_rescorer": {
                    "key": "test",
                    "prefix_field_name": "prefix",
                    "suffix_field": "idx",
                    "api_url": "dummy",
                    "base_score": 2
                }
            }
        return query
