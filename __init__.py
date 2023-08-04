
import logging
import time

logger = logging.getLogger('air_feather')
handler = logging.StreamHandler()
fmt = logging.Formatter('%(levelname)s: %(asctime)s: %(name)s %(filename)s'
                        '[line:%(lineno)d] %(funcName)s * %(process)d:%(thread)d %(message)s')
handler.setFormatter(fmt)
handler.setLevel(logging.INFO)

logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

class TaskState(object):
    """task state"""
    STOP = 0
    START = 1
    SCHEDULED = 2
    RUN = 3
    FINISH = 4
    FAILED = 5
    ERROR = 6


def helo(name, error=False):
    """helo func for test and example"""
    FOR_HELO_CHANGE.append(time.time())
    if error:
        print(f'helo error: {name}')
        raise RuntimeError('helo error')
    print(f'helo :{name}')
    time.sleep(1)
    return name


FOR_HELO_CHANGE = [] 

class StopFlag(object):
    """StopFlagObject for thread"""
    def __init__(self):
        self.stop = False
