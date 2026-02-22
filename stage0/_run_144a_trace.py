# -*- coding: utf-8 -*-

import logging
import sys
import gc

gc.collect()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

from _trace_settings import get_local_config
from clean_trace_local import clean_standard_trace

if __name__ == '__main__':
    cfg = get_local_config("144a")
    all_data = clean_standard_trace(**cfg)
