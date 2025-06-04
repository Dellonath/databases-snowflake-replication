import os 
import json
import logging
import datetime

CONFIG = json.loads(open('config.json').read())

now_timestamp = datetime.datetime.now()
LOG_FILE_PATH = (
    f"{CONFIG.get('logs_path')}"
    f'log_{now_timestamp.strftime(format='%H%M%S')}.csv'
)
os.makedirs(name=os.path.dirname(LOG_FILE_PATH), exist_ok=True)

logging.basicConfig(filename=LOG_FILE_PATH,
                    format='%(asctime)s.%(msecs)03d|%(levelname)s|%(message)s',
                    filemode='a',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

_log = logging.getLogger(__name__)
