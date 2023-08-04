from __future__ import absolute_import

import threading
import os
import sys
import logging
import socket
import threading
import time
import json
import collections
import traceback

import datetime
from concurrent import futures
from air_feather.base_storage import Task, Node, TaskLog
from air_feather import TaskState, logger
from sqlalchemy import update, select, insert, delete, and_

class ExecutorNode(object):
    """executor node impl run task"""
    def __init__(self, storage, node_id=None, node_name=None, resource=None, ping_interval=10):
        self.storage = storage
        self.alias = node_name
        self.resource = resource
        hostname = socket.gethostname()
        self.ip = socket.gethostbyname(hostname)
        self.process_id = os.getpid()
        self.node_id = node_id or  'ip:%s,pid:%s' % (self.ip, self.process_id)
        # TODO multi process and multi thread support 
        self.worker_pool = futures.ThreadPoolExecutor(max_workers=50, thread_name_prefix='worker-thread')

        self.ping_interval = ping_interval
        logger.info('node init:%s', self.node_id)
        
    def _register(self):
        """register cur new node to db"""
        logger.info('node register:%s', self.node_id)
        self.storage.register_node(self.node_id, self.alias, self.ip, self.process_id, self.resource)

    def _heartbeat(self):
        """heartbeat to db by update ping time"""
        logger.info('node heartbeat:%s', self.node_id)
        ret = self.storage.update_node(self.node_id)
        # 如果失败，代表节点已经被认为死亡，应该主动退出
        if not ret:
            logger.info('node heartbeat fail:%s', self.node_id)
            raise SystemExit('node heartbeat fail', self.node_id)
    
    def _worker_func(self, p_func, task):
        """worker func wrap real func"""
        # time.sleep(2)
        # TODO 对于持续任务，中途，任务停止，删除，修改，支持
        task_id = task.task_id
        exec_time = datetime.datetime.now()
        task.exec_time = exec_time 
        task_node_id = task.node_id
        task_dict_info = task.get_obj_dict()
        func_result_code = 0
        func_result_msg = ''
        if p_func is not None:
        
            # update exec_time
            with self.storage.Session() as session:
                # #add old object to new session by a new query, session track object by primary id
                # #session.merge(task)
                stmt = update(Task).where(Task.task_id == task_id, 
                                          Task.state == TaskState.RUN).values(exec_time=exec_time)
                tmp = session.execute(stmt)
                if tmp.rowcount != 1:
                    logger.info('task update exec_time fail, skip run, %s', task)
                    return
                session.commit()

            logger.info('task start run:%s, %s', task_id, p_func)

            try:
                ret = p_func()
            except Exception as e:
                state  = TaskState.FAILED
                logger.info('task running fail, state:%s,result:%s, %s', state, type(e), str(e))
                func_result_msg = traceback.format_exc()
                func_result_code = 1
            else:
                state = TaskState.FINISH
                logger.info('task running success state:%s,result:%s', state, ret)
                func_result_msg = ret
                func_result_code = 0
                
        else:
            state = TaskState.ERROR
            logger.info('task running error state:%s,result:task func parse fail', state)
            func_result_msg = 'task func parse fail'
            func_result_code = 2

        task_dict_info['func_result_code'] = func_result_code
        task_dict_info['func_result_msg'] = func_result_msg
        task_info = json.dumps(task_dict_info, default=str)

        with self.storage.Session() as session:
            try:
                # insert task log
                task_log_info = collections.OrderedDict(task_id=task_id, 
                                               task_info=task_info, 
                                               node_id=task_node_id, 
                                               action=state)
                stmt2 = insert(TaskLog).values(**task_log_info)
                ret = session.execute(stmt2)
                if ret.rowcount != 1:
                    logger.warning('insert task log fail:%s', task_log_info)
                # delete finish task
                ret2 = session.execute(delete(Task).where(
                    Task.task_id == task_id, Task.state == TaskState.RUN, Task.exec_time.isnot(None)))
                if ret2.rowcount != 1:
                    logger.warning('delete run finish task fail:%s', task)
                session.commit()
                logger.info('finish running:%s', task_id)
            except BaseException as e:
                logger.exception('worker func after deal fail')
                raise e

    def get_run_task(self):
        """get and run task"""
        # expire_on_commit session.commit 之后，对象仍然可以使用，但是得注意，可能属性过期
        # 如果不适用这个，获取属性设置属性，会卡住或报错
        with self.storage.Session(expire_on_commit=False) as session:
            stmt = select(Task).filter(Task.state == TaskState.SCHEDULED, Task.node_id == self.node_id)
            tasks = session.execute(stmt).scalars().all()
            if not tasks:
                return
            logger.info('get run task:%s', tasks)
            for task in tasks:
                # update to run, if success then submit, else pass
                stmt = update(Task).where(Task.task_id == task.task_id, Task.state == TaskState.SCHEDULED).values(
                    state=TaskState.RUN
                )
                update_ret = session.execute(stmt)
                logger.debug('xxxxxxxxxxxxxxx%s', task.node_id)
                # success
                if update_ret.rowcount == 1:
                    try:
                        p_func = task.get_task_info()
                    except Exception as e:
                        logger.exception('parse task func error')
                        p_func = None
                    logger.info('task submit:%s', task)
                    self.worker_pool.submit(self._worker_func, p_func, task)
            session.commit()

    def schedule_run_task(self, stopflag):
        """schedule cur node task to run"""
        while not stopflag.stop:
            try:
                self.get_run_task()
            except Exception as e:
                logger.exception('worker schedule run task fail')
                raise e
            time.sleep(0.1)
        logger.warning('schedule run task thread exit')
    
    def run(self, stopflag):
        """worker node run"""
        # register cur worker node to db
        self._register()

        # schedule cur node task to run thread
        schedule_run_task_th = threading.Thread(target=self.schedule_run_task, args=(stopflag,))
        schedule_run_task_th.daemon = True
        schedule_run_task_th.start()
        routine_yield = routine(self.ping_interval)

        while not stopflag.stop and schedule_run_task_th.is_alive() and next(routine_yield):
            try:
                self._heartbeat()
            except BaseException:
                logger.exception('worker heartbeat fail')
                break

        logger.error('worker node end with fail, sys.exit')
        sys.exit('worker node end with fail, sys.exit')
            

def routine(interval=5):
    """A routine generator, yields every `interval` seconds.

    Args:
        interval (int, optional): routine interval in seconds. Defaults to 5.

    Yields:
        (double, int): current time, and routine iteration
    """
    interval = int(interval) if interval > 1 else 1
    # sleep_interval = interval / 10.0
    last_run = 0
    while True:
        now = time.time()
        curr_run = int(now) // interval
        
        if last_run >= curr_run:
            sleep_time = (last_run + 1) * interval - time.time()
            time.sleep(sleep_time)
            continue
        last_run = curr_run
        yield now, curr_run
