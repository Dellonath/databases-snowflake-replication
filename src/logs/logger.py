import logging
import datetime
import os 

os.makedirs(name='logs', exist_ok=True)

timestamp = datetime.datetime.now(tz='UTC').strftime(format="%Y%m%d%H%M%S")
logging.basicConfig(filename=f'logs/extraction_{timestamp}.log', 
                    format='%(asctime)s %(levelname)s %(message)s',
                    filemode='a',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

_log = logging.getLogger(__name__)
