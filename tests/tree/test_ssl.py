import os

import yaml

from iotdb.Session import Session
from iotdb.SessionPool import PoolConfig, SessionPool
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor

"""
 Title：测试树模型Session连接超时参数
 Describe：基于树模型，测试Session连接超时参数
 Author：肖林捷
 Date：2025/03/06
"""

# 测试Session的SSL连接
def test_session_ssl_connection():

    # open and read yaml files
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'conf',
        'config.yml')
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    if config['enable_thrift_ssl'] is False:
        return

    # create stand-alone session connection
    session = Session(
        host=config['host'],
        port=config['port'],
        user=config['username'],
        password=config['password'],
        use_ssl=config['use_ssl'],
        ca_certs=config['ca_certs'],
    )
    # open session connection
    session.open(False)

    # create databases
    session.set_storage_group("root.test_session_ssl_connection_db1")
    # setting time series.
    session.create_time_series(
        "root.test_session_ssl_connection_db1.d1.s1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY
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
    session.delete_storage_group("root.test_session_ssl_connection_db1")

    # close session connection
    session.close()

# 测试SessionPool的SSL连接
def test_session_pool_ssl_connection():
    # open and read yaml files
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'conf',
        'config.yml')
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    if config['enable_thrift_ssl'] is False and config['enable_session_pool'] is False:
        return

    pool_config = PoolConfig(
        node_urls=config['node_urls'],
        user_name=config['username'],
        password=config['password'],
        use_ssl=config['use_ssl'],
        ca_certs=config['ca_certs'],
    )
    max_pool_size = 5
    wait_timeout_in_ms = 3000

    # Create a session pool
    session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)

    # get a session
    session = session_pool.get_session()

    # create databases
    session.set_storage_group("root.test_session_pool_ssl_connection_db1")
    # setting time series.
    session.create_time_series(
        "root.test_session_pool_ssl_connection_db1.d1.s1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY
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
    session.delete_storage_group("root.test_session_pool_ssl_connection_db1")

    # recovery session connection
    session_pool.put_back(session)
    # close session_pool connection
    session_pool.close()

# 测试SessionCluster的SSL连接
def test_session_cluster_ssl_connection():

    # open and read yaml files
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'conf',
        'config.yml')
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    if config['enable_thrift_ssl'] is False and config['enable_cluster'] is False:
        return

    # create session connection
    session = Session.init_from_node_urls(
        node_urls=config['node_urls'],
        user=config['username'],
        password=config['password'],
        use_ssl=config['use_ssl'],
        ca_certs=config['ca_certs'],
    )
    # open session connection
    session.open(False)

    # create databases
    session.set_storage_group("root.test_session_cluster_ssl_connection_db1")
    # setting time series.
    session.create_time_series(
        "root.test_session_cluster_ssl_connection_db1.d1.s1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY
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
    session.delete_storage_group("root.test_session_cluster_ssl_connection_db1")

    # close session connection
    session.close()
