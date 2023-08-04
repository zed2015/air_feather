from __future__ import absolute_import

import threading
import os
import sys
import logging
from air_feather.base_storage import Task, Node, MysqlStorage
from air_feather import TaskState, logger, StopFlag
from sqlalchemy import select, update, orm, or_, delete, and_, text
from air_feather.executor import ExecutorNode
from sqlalchemy.exc import IntegrityError

import json
import datetime
import threading
import time

class SchedulerNode(object):
    """ scheduler node, schedule due task to alive node"""
    def __init__(self, storage, node_ping_interval):
        self.storage = storage
        self.node_ping_interval = node_ping_interval
        self.dead_node_interval = self.node_ping_interval * 2

    # def register_task(self):
    #     pass
    # def stop_task(self):
    #     pass
    # def start_task(self):
    #     pass
    # def restart_task(self):
    #     pass
    # def drop_task(self):
    #     pass

    # def status_task(self, key):
    #     pass
    
    def clear_dead_node(self):
        """clear dead node and change task state to start for another schedule"""
        logger.debug('clear_dead_node')
        with self.storage.Session() as session:
            now = datetime.datetime.now()
            #stmt = select(Node).filter(Node.latest_ping < now - datetime.timedelta(seconds=self.dead_node_interval))
            stmt = select(Node).filter(Node.latest_ping < text('DATE_SUB(CURRENT_TIMESTAMP, INTERVAL %s SECOND)' % self.dead_node_interval))
            dead_node = session.execute(stmt).scalars().all()
            if not dead_node:
                return
            
            dead_node_ids = []
            for node in dead_node:
                dead_node_ids.append(node.node_id)
            

            ret = session.execute(
                update(Task).where(Task.node_id.in_(dead_node_ids), 
                                   Task.state.in_([TaskState.RUN, TaskState.SCHEDULED])
                                   ).values(state=TaskState.START, node_id=None))
            if ret.rowcount != 0:
                logger.info('delete dead node, recover task:%s, %s', dead_node_ids, ret.rowcount)

            ret = session.execute(delete(Node).where(Node.node_id.in_(dead_node_ids)))
            logger.info('delete dead node, info:%s, count:%s', dead_node_ids, ret.rowcount)
            session.commit()
                
    def run_schedule(self, stop_flag_obj: StopFlag, backgound: bool=False):
        """schedule node run schedule"""
        while not stop_flag_obj.stop:
            with self.storage.Session() as session:
                ret = self.storage.get_lock(session, 'task_schedule')
                logger.debug('get schedule lock:%s, %s', os.getpid(), ret)
                try:
                    self.run_once()
                except Exception as e:
                    logger.exception('schedule node run once fail')
                    raise e
            time.sleep(0.1)
        logger.warning('schedule node stop exit')
            
                
    def run_once(self):
        """run once schedule"""
        self.clear_dead_node()

        # query due task
        now = datetime.datetime.now()

        with self.storage.Session() as session:
            due_tasks = session.execute(
                select(Task).filter(Task.state == TaskState.START, Task.next_run_time <= now)
                ).scalars().all()
            logger.debug('due tasks:%s', due_tasks) 

            # collect all node
            alive_node_sql = select(Node).select_from(Node).outerjoin(Node.task)
            alive_node_sql = alive_node_sql.options(orm.contains_eager(Node.task)).filter(
                or_(Task.state == TaskState.RUN, Task.state.is_(None), Task.state == TaskState.SCHEDULED)
                # or_(Task.state.in_((TaskState.RUN, TaskState.SCHEDULED)), Task.state == None)
                ).filter(
                    Node.latest_ping > now - datetime.timedelta(seconds=self.dead_node_interval)
                )
            logger.debug('alive node sql:%s', alive_node_sql)
            alive_nodes = session.execute(alive_node_sql).unique().scalars().all()
            logger.debug('alive node:%s', alive_nodes)

            node_left_resource_info = {}
            node_id_map_node = {}
            for x in alive_nodes:
                # logger.debug(x.task)
                node_id_map_node[x.node_id] = x
                if x.resource:
                    node_resouce_info = json.loads(x.resource)
                else:
                    # default
                    node_resouce_info = {'global': 1000}

                for k, v in node_resouce_info.items():
                    node_left_resource_info.setdefault(x.node_id, {})[k] = [int(v), int(v)]

                for x_running_task in x.task:
                    if x_running_task.resource:
                        task_resource_info = json.loads(x_running_task.resource)
                    else:
                        task_resource_info = {'global': 1}
                    for k, v in task_resource_info.items():
                        node_left_resource_info[x.node_id][k][0] = node_left_resource_info[x.node_id][k][0] - v

            # distribute due task to node
            for due_task in due_tasks:
                if due_task.resource:
                    task_resource_info = json.loads(due_task.resource)
                else:
                    task_resource_info = {'global': 1}
                
                dst_node_id = None
                dst_node_left_resource_score = 0
                for node_id in node_left_resource_info.keys():
                    ok_flag = True
                    left_resource_score = 0
                    for k, v in task_resource_info.items():
                        if k in node_left_resource_info[node_id] and v <= node_left_resource_info[node_id][k][0]:
                            # TODO 不同的权重
                            left_resource_score += node_left_resource_info[node_id][k][0] * 1.0 \
                                / node_left_resource_info[node_id][k][1]
                            ok_flag = True
                        else:
                            ok_flag = False
                    if ok_flag and left_resource_score > dst_node_left_resource_score:
                        dst_node_id = node_id
                        dst_node_left_resource_score = left_resource_score

                
                if dst_node_id is None:
                    logger.warning('task can not be scheuled, because no adequate resoruce node, %s', due_task)
                else:
                    # avoid task has been updated
                    dis_stmt = update(Task).where(Task.task_id == due_task.task_id, 
                                                  Task.state == TaskState.START).values(
                        node_id=dst_node_id, state=TaskState.SCHEDULED)
                    update_ret = session.execute(dis_stmt)
                    if update_ret.rowcount == 1:
                        logger.info('task be scheduled to node, %s, %s', due_task, node_id_map_node[dst_node_id])
                        for k, v in task_resource_info.items():
                            node_left_resource_info[dst_node_id][k][0] = node_left_resource_info[dst_node_id][k][0] - v
                    else:
                        logger.info('task scheduled fail, update sql fail, %s', due_task)
            # TODO 验证多次提交               
            session.commit()


class Scheduler(object):
    """scheduler, all entrypoint"""
    def __init__(self, storage_args, node_ping_interval=5, node_args=None):

        self.storage = MysqlStorage(**storage_args)
        self.schedule_node = SchedulerNode(self.storage, node_ping_interval)
        node_args = node_args or {}
        self.excutor_node = ExecutorNode(self.storage, ping_interval=node_ping_interval, **node_args)
        self.stopflag = None
    
    def add_task(self, task_id, func, args=None, kwargs=None, alias=None, next_run_time=None, func_retry_options=None,
                replace_existing=False):
        """add task
        func_retry_options please refer to retrying module,
        now, only support {'stop_max_attempt_number', 'stop_max_delay',
                    'wait_fixed',
                    'wait_random_min', 'wait_random_max',
                    'wait_incrementing_start', 'wait_incrementing_increment',
                    'wait_exponential_multiplier', 'wait_exponential_max'}
        """
        
        task_desc = Task.get_task_desc(func, args, kwargs, func_retry_options=func_retry_options)
        alias = alias or func.__name__
        resource = None
        state = TaskState.START
        next_run_time = next_run_time or datetime.datetime.now()
        
        
        data = {
            "task_id": task_id,
            "task_desc": task_desc,
            "resource": resource,
            "state": state,
            "alias": alias,
            'next_run_time': next_run_time
        }
        from sqlalchemy.dialects.mysql import insert
        insert_stmt = insert(Task).values(**data)
        on_d_k_stmt = insert_stmt.on_duplicate_key_update(**data)
        logger.debug(on_d_k_stmt)
        with self.storage.Session() as session:
            if replace_existing:
                # TODO 限定只能更新START 状态的
                ret = session.execute(on_d_k_stmt)
            else:
                ret = session.execute(insert_stmt)
            session.commit()
        return ret.rowcount
            

        # task = Task.task_id=task_id, task_desc=task_desc, resource=resource, state=state, alias=alias)
        # task.next_run_time = next_run_time or datetime.datetime.now()
        # try:
        #     self.storage.register_task(task)
        # except IntegrityError as e:
        #     if replace_existing:
        #         self.storage.update_task(task)
        #     else:
        #         raise e
        # return task
    
    def query_task(self, task_id):
        """query task by id"""
        return self.storage.get_task(task_id)
    
    def query_task_by_id_prefix(self, v):
        """query_task_by_id_like"""
        return self.storage.query_task(Task.task_id.startswith(v))
    
    def delete_task(self, task_id):
        """delete_task"""
        # TODO 持续人任务，删除/停止，如何停止？
        ret = self.storage.delete_task(task_id)
        if ret:
            logger.info('delete task success:%s', task_id)
        return ret
    
    def update_task_run_time(self, task_id, run_time):
        """update_task_run_time"""
        ret = self.storage.update_task_run_time(task_id, run_time)
        if ret:
            logger.info('update task run time success:%s, %s', task_id, run_time)
        return ret
    
    def run(self, stopflag: StopFlag=None, schedule=True, worker=True, blocking=True):
        """run"""
        if stopflag is None:
            self.stopflag = stopflag = StopFlag()
            
        logger.info('main start')
        if not schedule and not worker:
            logger.info('schedule and worker must have one')
            sys.exit(1)

        ths = []
        if schedule:
            logger.info('start scheuler node')
            schedule_node_th = threading.Thread(target=self.schedule_node.run_schedule, args=(stopflag, ))
            ths.append(schedule_node_th)
        if worker:
            logger.info('start worker node')
            worker_node_th = threading.Thread(target=self.excutor_node.run, args=(stopflag, ))
            ths.append(worker_node_th)
        
        for th in ths:
            th.daemon = True
            th.start()
        
        # if blocking:
        #     logger.info('run main ...')
        #     while 1:
        #         time.sleep(10)

        while not stopflag.stop:
            for th in ths:
                if not th.is_alive():
                    logger.warning('important thread is dead, exit process')
                    os._exit(1)
            time.sleep(0.1)
        logger.warning('main sche stop exit')
        
        
        


