try:
    from sqlalchemy import (
        create_engine, Table, Column, MetaData, Unicode, Float, LargeBinary, select, and_, Text, BigInteger,
        ForeignKey)
    from sqlalchemy.orm import relationship
    from sqlalchemy import Integer, Text, DateTime, text, select, update, delete, and_, or_
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.sql.expression import null
    from sqlalchemy.dialects import mysql
    
except ImportError:  # pragma: nocover
    raise ImportError('SQLAlchemyJobStore requires SQLAlchemy installed')

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from air_feather import logger
from air_feather import TaskState

import datetime
import json
import importlib
import functools
import urllib.parse
import copy
from retrying import retry

"""
工作节点状态表
NodeStat:
create table af_node(
id int primary key auto_incretment
node_id varchar(254) not null,
alias varchar(254) default null,
ip  varchar(128) not null,
process_id varchar(64) not null,
resource  text default null,
last_ping_ts_ms datetime(3) not null,
`create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
unique(node_id)
)
====================
任务表 task
create table af_task(
id int primary key auto_incretment,
task_id varchar(254)  not null,
alias varchar(254) default null,
task_desc text not null,
resource text default null,
state int not null,
node_id varchar(254) default null,
next_run_time datetime(3) default current_timestamp,
exec_time datetime(3) default null,
`create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
unique(task_id)

)
=================
任务记录表 task_log
create table af_task_log(
id int primary key auto_incretment,
task_id varchar(254) not null,
task_info text not null,
node_id varchar(254) not null,
action int not null,
action_time datetime(3) default null,
`create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
)

======================
TODO 节点上线，下线日志
节点记录表 node_log
create table af_node_log(
id int primary key auto_incretment,
node_id varchar(254) not null,
node_alias varchar(254) default null,
action int not null,
action_time datetime(3) default null,
`create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
)

"""



def todict(obj):
    """ Return the object's dict excluding private attributes, 
    sqlalchemy state and relationship attributes.
    """
    excl = ('_sa_adapter', '_sa_instance_state')
    return {k: v for k, v in vars(obj).items() if not k.startswith('_') and
            not any(hasattr(v, a) for a in excl)}

class BaseB:
    create_time = Column('create_time', mysql.DATETIME(fsp=3), server_default=text('CURRENT_TIMESTAMP(3)'),
            nullable=False)
    update_time = Column('update_time', mysql.DATETIME(fsp=3), server_default=text('CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)'), index=True,
            nullable=False)

    def __repr__(self):
        params = ', '.join(f'{k}={v}' for k, v in todict(self).items())
        return f"{self.__class__.__name__}({params})"
    
    def get_obj_dict(self):
        """get obj dict"""
        return todict(self)
    
    
Base = declarative_base(cls=BaseB)

DEFAULT_CURRENT_TIME_SQL = 'CURRENT_TIMESTAMP(3)'


class Task(Base):
    """Task"""
    __tablename__ = 'af_task'
    
    id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    task_id = Column('task_id', Unicode(254, _warn_on_bytestring=False), unique=True)
    alias = Column('alias', Unicode(254, _warn_on_bytestring=False), default=None)
    task_desc = Column('task_desc', Text(), nullable=False)
    resource = Column('resource', Text(), default=None)
    state = Column('state', Integer(), nullable=False)
    # node_id = Column('node_id', Unicode(254, _warn_on_bytestring=False), ForeignKey('af_node.id'), default=None)
    # 数据库不实际创建外键;
    node_id = Column('node_id', Unicode(254, _warn_on_bytestring=False), default=None)
    next_run_time = Column('next_run_time', mysql.DATETIME(fsp=3), server_default=text('CURRENT_TIMESTAMP(3)'), index=True,
            nullable=True)
    exec_time = Column('exec_time', mysql.DATETIME(fsp=3), default=None,
            nullable=True)
    
    node = relationship('Node', back_populates='task', foreign_keys=[node_id], primaryjoin='Task.node_id == Node.node_id')

    @staticmethod
    def get_task_desc(func, args=None, kwargs=None, func_retry_options=None):
        func_retry_options = func_retry_options or {}
        options = {'stop_max_attempt_number', 'stop_max_delay',
                    'wait_fixed',
                    'wait_random_min', 'wait_random_max',
                    'wait_incrementing_start', 'wait_incrementing_increment',
                    'wait_exponential_multiplier', 'wait_exponential_max'}
        func_retry_options_key = set(func_retry_options.keys())
        assert func_retry_options_key.issubset(options), \
            'func_retry_options only support {}, yours:{}'.format(options, func_retry_options_key)
            
        task_desc = json.dumps({
            'func':  func.__module__ + '.' + func.__name__,
            'args': args or (),
            'kwargs': kwargs or {},
            'func_retry_options': func_retry_options
        })
        return task_desc

    
    def get_task_info(self):
        info = json.loads(self.task_desc)
        module_name, func_name = info['func'].rsplit('.', maxsplit=1)
        module = importlib.import_module(module_name)
        try:
            func = getattr(module, func_name)
        except AttributeError:
            raise
        func_retry_options = info.get('func_retry_options')
        if func_retry_options:
            func = retry(**func_retry_options)(func)

        partial_func = functools.partial(func, *info['args'], **info['kwargs'])
        return partial_func
    
        
        
        
class TaskLog(Base):
    """Task"""
    __tablename__ = 'af_task_log'
    # id int primary key auto_incretment,
    # task_id varchar(254) not null,
    # task_info text not null,
    # node_id varchar(254) not null,
    # action int not null,
    # action_time datetime(3) default null
    
    id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    task_id = Column('task_id', Unicode(254, _warn_on_bytestring=False))
    task_info = Column('task_info', Text(), nullable=False)
    node_id = Column('node_id', Unicode(254, _warn_on_bytestring=False), default=None)
    action = Column('action', Integer(), nullable=False)
    action_time = Column('action_time', mysql.DATETIME(fsp=3), nullable=False, default=datetime.datetime.now)

class Node(Base):
    """Node"""
    __tablename__ = 'af_node'
    id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    node_id = Column('node_id', Unicode(254, _warn_on_bytestring=False), unique=True)
    alias = Column('alias', Unicode(254, _warn_on_bytestring=False), default=None)
    ip =  Column('ip', Unicode(128, _warn_on_bytestring=False), nullable=False)
    process_id = Column('process_id', Unicode(64, _warn_on_bytestring=False), nullable=False)
    resource =  Column('resource', Text(), default=None)
    latest_ping =  Column('latest_ping', mysql.DATETIME(fsp=3), server_default=text('current_timestamp(3)'),
                nullable=False)
    task = relationship('Task', back_populates='node', primaryjoin='foreign(Task.node_id) == Node.node_id')
    
    # def __str__(self):
    #     return str(self.__dict__)

class Lock(Base):
    """Node"""
    __tablename__ = 'af_lock'
    id = Column('id', Unicode(254, _warn_on_bytestring=False), primary_key=True)
    alias = Column('alias', Unicode(254, _warn_on_bytestring=False), default=None)

class BaseStorage(object):
    """
    base storage
    """
    def __init__(self):
        pass


    # def register_node(self):
    #     pass

    # def update_node(self):
    #     pass 

    # def collect_node(self):
    #     pass

    # def register_task(self):
    #     pass

    # def update_task(self):
    #     pass

    # def get_task(self):
    #     pass
    
    # def collect_task(self):
    #     pass

    # def distribute_task(self):
    #     pass


class MysqlStorage(BaseStorage):
    """
    mysql impl for storage
    """
    def __init__(self, url, engine_options=None, url_driver='mysql+pymysql', *args, **kwargs):
        if isinstance(url, dict):
            url = copy.deepcopy(url)
            url['user'] = urllib.parse.quote(url['user'])
            url['passwd'] = urllib.parse.quote(url['passwd'])
            try:
                url = '{url_driver}://{user}:{passwd}@{host}:{port}/{db}'.format(url_driver=url_driver, **url)
            except KeyError as e:
                logger.error('mysql storage url error, key error %s', e)
                raise e
        self.engine = create_engine(url, **(engine_options or {}))
        self.Session = sessionmaker(self.engine)
        Base.metadata.create_all(self.engine)
        lock_ids = ['task_schedule']
        for x in lock_ids:
            with self.Session() as session:
                lock = Lock(id=x)
                session.add(lock)
                try:
                    session.commit()
                except IntegrityError:
                    logger.info('has exist:%s', lock)
        # if not self.node_t.exists(self.engine):
        #     self.node_t.create(self.engine, False)
    
    def get_lock(self, session, lock_name):
        """get write lock"""
        return session.execute(select(Lock).filter(Lock.id==lock_name).with_for_update()).all()

    def register_node(self, node_id, alias, ip, process_id, resource=None):
        """register node to db"""
        new_node = Node(node_id=node_id, alias=alias, ip=ip, process_id=process_id, resource=resource)
        with self.Session() as session:
            session.add(new_node)
            session.commit()
    
    def update_node(self, node_id):
        """update node lastest time"""
        stmt = update(Node).where(Node.node_id == node_id).values(latest_ping=text('current_timestamp(3)'))
        with self.Session() as session:
            ret = session.execute(stmt)
            session.commit()
        if ret.rowcount == 0:
            return False
        else:
            return True
        
    def collect_node(self):
        """collect all node"""
        with self.Session() as session:
            ret = session.query(Node).all()        
            return ret
    
    def register_task(self, task):
        """register task"""
        with self.Session() as session:
            session.add(task)
            session.commit()
    
    def update_task(self, task):
        """update task"""
        with self.Session() as session:
            stmt = update(Task).where(Task.task_id==task.task_id).values(alias=task.alias, task_desc=task.task_desc,
                resource=task.resource, state=task.state, node_id=task.node_id, next_run_time=task.next_run_time,
                exec_time=task.exec_time)
            session.execute(stmt)
            session.commit()
    
    def update_task_value(self, *where_clause, **kwargs):
        """update task by value"""
        with self.Session() as session:
            stmt = update(Task).where(*where_clause).values(**kwargs)
            session.execute(stmt)
            session.commit()
    
    def update_task_run_time(self, task_id, new_run_time):
        """update task run time"""
        with self.Session() as session:
            stmt = update(Task).where(and_(Task.task_id==task_id, Task.state!=TaskState.SCHEDULED, Task.state!=TaskState.RUN)).values(next_run_time=new_run_time)
            logger.debug('update run time, sql:%s', stmt)
            ret = session.execute(stmt)
            session.commit()
        return ret.rowcount

    
    def get_task(self, task_id):
        """get task"""
        with self.Session() as session:
            stmt = select(Task).filter(Task.task_id==task_id)
            ret = session.execute(stmt).scalars().one_or_none()
            return ret
    
    def query_task(self, *where_clause):
        """query task by condition"""
        with self.Session() as session:
            stmt = select(Task).filter(*where_clause)
            return session.execute(stmt).scalars().all()
            
    def query_task_log(self, task_id):
        """query task by condition"""
        with self.Session() as session:
            stmt = select(TaskLog).filter(TaskLog.task_id.startswith(task_id))
            return session.execute(stmt).scalars().all()
        
    def delete_task_log(self, task_id):
        """query task by condition"""
        with self.Session() as session:
            stmt = delete(TaskLog).where(TaskLog.task_id.startswith(task_id)).execution_options(synchronize_session=False)
            ret = session.execute(stmt)
            session.commit()
        return ret.rowcount
    
    
    def delete_task(self, task_id):
        with self.Session() as session:
            stmt = delete(Task).where(Task.task_id==task_id)
            ret = session.execute(stmt)
            session.commit()
        return ret.rowcount

    
    def get_scheduled_task(self, node_id):
        """"get scheduled task"""
        with self.Session() as session:
            stmt = select(Task).filter(Task.state==TaskState.SCHEDULED, Task.node_id==node_id)
            tasks = session.execute(stmt).scalars().all()
            return tasks
    
