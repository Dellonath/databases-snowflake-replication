import logging
import datetime
import os 

timestamp = datetime.datetime.now().strftime(format="%Y_%m_%d_%H_%M_%S")
_log = logging.getLogger(__name__)

os.makedirs(name='logs', exist_ok=True)
logging.basicConfig(filename=f'logs/extraction_{timestamp}.log', 
                    format='%(asctime)s %(levelname)s %(message)s',
                    filemode='a',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
