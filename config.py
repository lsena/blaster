from pydantic import BaseSettings

import os


class ClusterSettings(BaseSettings):
    app_name: str = "Blaster"
    connection_string: str = os.getenv('CONN', "es:9200")


class IndexSettings(BaseSettings):
    refresh_interval = 60  # seconds
    replica_number = 3
    buffer_size = 1024  # MB
    translog_sync_interval = 5000  # ms
    translog_durability = 'async'  # request/async
    translog_flush_threshold_size = 1024  # MB


class DocSettings(BaseSettings):
    app_name: str = "Awesome API"


class ClientSettings(BaseSettings):
    bulk_size: int = 1000


class Settings(BaseSettings):
    app_name: str = "Blaster"
    sqs_queue_url: str = os.getenv('SQS_QUEUE_URL')
    num_proc = os.cpu_count()
    proc_concurrency = 1
    num_docs = 100
    cluster_settings = ClusterSettings()
    index_settings = IndexSettings()
    doc_settings = DocSettings()
    tmp_data_folder = '/tmpdir'
    static_data_folder = 'data'


settings = Settings()
