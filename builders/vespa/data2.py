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
    mapping_path = f'{get_settings().static_data_folder}/vespa/schema/data2/application'
