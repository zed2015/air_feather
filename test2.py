from air_feather.base_storage import MysqlStorage, Task, Node
from air_feather.scheduler import SchedulerNode, Scheduler
from sqlalchemy import select
import time
from collections import OrderedDict

def helo(name):
    """helo"""
    time.sleep(5)
    print('helo' + name)
    return 1

if __name__ == '__main__':
    url = 'mysql://user:passs@127.0.0.1:3306/zed'
    import sqlalchemy
    print(sqlalchemy.__version__)
    # store = MysqlStorage(url=url, engine_options={'echo': 0})
    storage_args = OrderedDict(url=url, engine_options={'echo': 0})
    
    sche = Scheduler(storage_args=storage_args)
    # sche.run(schedule=True, worker=True)
    # sche.schedule_node.run_schedule()
    new_task = sche.add_task('helo001', helo, ('001', ), replace_existing=1)
    # new_task = sche.add_task('helo002', helo2, ('001', ), replace_existing=1)
