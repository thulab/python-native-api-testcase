import os

import yaml
from iotdb.Session import Session
from iotdb.SessionPool import PoolConfig, SessionPool
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor

"""
 Title：测试树模型Session连接超时参数
 Describe：基于1.3.4.2版本树模型，测试Session连接超时参数，需要手动测试，开启配置文件中enable_thrift_ssl，并配置好IoTDB
 Author：肖林捷
 Date：2025/03/06
"""

# tests the parameters of python session sdk connection timeout
def test_connection_time_out():
   # open and read yaml files
   config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'conf', 'config.yml')
   with open(config_path, 'r', encoding='utf-8') as file:
      config = yaml.safe_load(file)

   connection_timeout = None

   # determine whether to enable session pool configuration
   if config['enable_session_pool']:
      pool_config = PoolConfig(
         node_urls=config['node_urls'],
         user_name=config['username'],
         password=config['password'],
         connection_timeout_in_ms=connection_timeout,
      )
      max_pool_size = 5
      wait_timeout_in_ms = 3000

      # Create a session pool
      session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)

      # get a session
      session = session_pool.get_session()
   else:
      # determine whether to enable cluster configuration
      if config['enable_cluster']:
         # create cluster session connection
         session = Session.init_from_node_urls(
            node_urls=config['node_urls'],
            user=config['username'],
            password=config['password'],
            connection_timeout_in_ms=connection_timeout,
         )
      else:
         # create stand-alone session connection
         session = Session(
            config['host'],
            config['port'],
            config['username'],
            config['password'],
            connection_timeout_in_ms=connection_timeout,
         )
      # open session connection
      session.open(False)

   # create databases
   session.set_storage_group("root.test_connection_timeout_db1")
   # setting time series.
   session.create_time_series(
      "root.test_connection_timeout_db1.d1.s1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY
   )
   # execute non-query sql statement
   session.execute_non_query_statement(
      "insert into root.test_connection_timeout_db1.d1(timestamp, s1) values(1, \"success\")"
   )
   # execute sql query statement
   with session.execute_query_statement(
           "select s1 from root.test_connection_timeout_db1.d1"
   ) as session_data_set:
      session_data_set.set_fetch_size(1024)
      while session_data_set.has_next():
         print(session_data_set.next())
   # delete database
   session.delete_storage_group("root.test_connection_timeout_db1")

   if config['enable_cluster']:
      # recovery session connection
      session_pool.put_back(session)
      # close session_pool connection
      session_pool.close()
   else:
      # close session connection
      session.close()

