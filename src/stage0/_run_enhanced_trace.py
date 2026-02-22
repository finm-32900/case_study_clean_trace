# -*- coding: utf-8 -*-

import sys
from pathlib import Path

# -- path setup: sibling imports (src/stage0/) and parent imports (src/) --
_THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_THIS_DIR))
sys.path.insert(0, str(_THIS_DIR.parent))

import logging
import gc

gc.collect()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

from _trace_settings import get_local_config
from clean_trace_local import clean_enhanced_trace

if __name__ == '__main__':
    cfg = get_local_config("enhanced")
    all_data = clean_enhanced_trace(**cfg)
