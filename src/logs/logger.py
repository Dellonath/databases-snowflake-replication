import os
import json
import time
import logging
import datetime

CONFIG = json.loads(open('config.json').read())

timestamp = datetime.datetime.now(tz=datetime.timezone.utc).strftime(format='%Y%m%d%H%M%S')
LOG_FILE_PATH = (
    f"{CONFIG.get('logs_path')}"
    f'_logs_{timestamp}.log'
)
os.makedirs(name=os.path.dirname(LOG_FILE_PATH), exist_ok=True)

# setting logger to use UTC (GMT) time
logging.Formatter.converter = time.gmtime
logging.basicConfig(filename=LOG_FILE_PATH,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
                    filemode='a',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

_log = logging.getLogger(__name__)
