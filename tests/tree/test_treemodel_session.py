import os
from datetime import date

import pytest
import yaml
from iotdb.Session import Session
from iotdb.SessionPool import PoolConfig, SessionPool
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet

"""
 Title：测试树模型Session连接
 Describe：测试Session
 Author：肖林捷
 Date：2025/03/06
"""

# 配置文件目录
config_path = "../conf/config.yml"

# 读取配置文件
def read_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

# 验证Session有效性
def check_session_validity1(session):
    expect = 0
    actual = 0
    database_name_ = "root.test_session"
    device_name_ = database_name_ + "." + "d1"
    measurements_ = [
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "STRING", "BLOB", "DATE", "TS"
    ]
    data_type_lst_ = [
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
        TSDataType.STRING,
        TSDataType.BLOB,
        TSDataType.DATE,
        TSDataType.TIMESTAMP
    ]
    session.set_storage_group(database_name_)
    ts_path_lst_ = [device_name_ + "." + measurements_[i] for i in range(len(measurements_))]
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(measurements_))]
    compressor_lst_ = [Compressor.LZ4 for _ in range(len(measurements_))]
    session.create_multi_time_series(
        ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
    )
    values_ = []
    timestamps_ = []
    for i in range(10):
        values_.append([True, i * -100, i * 100, i * -0.123, i * 0.123, "text" + str(i), "string" + str(i), ("blob" + str(i)).encode('utf-8'), date(1970, 1, 1), i * 100])
        timestamps_.append(i)
        expect += 1

    tablet_ = Tablet(
        device_name_, measurements_, data_type_lst_, values_, timestamps_
    )
    session.insert_tablet(tablet_)
    with session.execute_query_statement(
            "select * from " + device_name_
    ) as session_data_set:
        session_data_set.set_fetch_size(1024)
        while session_data_set.has_next():
            session_data_set.next()
            actual += 1

    assert expect == actual, "Actual number does not match, expected:" + str(expect) + ", actual:" + str(actual)
    session.execute_non_query_statement("DELETE DATABASE root.**")


# 测试 fixture：测试环境清理
@pytest.fixture()
def fixture_():
    try:
        # 读取配置文件
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)

        # 建立连接
        session = Session(
            config['host'],
            config['port'],
            config['username'],
            config['password'])
        session.open()

        # 清理环境
        session.execute_non_query_statement("DELETE DATABASE root.**")

        # 关闭 session
        session.close()
    except Exception as e:
        assert False, "There is an error:" + str(e)

    yield
    # 用例执行完成后清理环境代码

    try:
        # 读取配置文件
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)

        # 建立连接
        session = Session(
            config['host'],
            config['port'],
            config['username'],
            config['password'])
        session.open()

        # 清理环境
        session.execute_non_query_statement("DELETE DATABASE root.**")

        # 关闭 session
        session.close()
    except Exception as e:
        assert False, "There is an error:" + str(e)

# 1、测试session连接：最简构造
@pytest.mark.usefixtures('fixture_')
def test_session1():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    session = Session(
        config['host'],
        config['port'],
    )
    session.open(False)
    check_session_validity1(session)
    session.close()

# 2、测试session连接：最繁构造
@pytest.mark.usefixtures('fixture_')
def test_session2():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    session = Session(
        config['host'],
        config['port'],
        config['username'],
        config['password'],
        fetch_size=5000,
        zone_id='+08:00',
        enable_redirection=True,
        use_ssl=False,
        ca_certs=None,
        connection_timeout_in_ms=None
    )
    session.open(False)
    check_session_validity1(session)
    session.close()

# 3、测试session连接：多次open
@pytest.mark.usefixtures('fixture_')
def test_session3():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    session = Session(
        config['host'],
        config['port'],
        user=config['username'],
        password=config['password'],
    )
    for i in range(10):
        session.open(False)
    check_session_validity1(session)
    session.close()

# 4、测试session连接：多次close
@pytest.mark.usefixtures('fixture_')
def test_session4():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    session = Session(
        config['host'],
        config['port'],
        user=config['username'],
        password=config['password'],
    )
    session.open(False)
    check_session_validity1(session)
    for i in range(10):
        session.close()

# 5、测试session连接：close后再open
@pytest.mark.usefixtures('fixture_')
def test_session5():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    session = Session(
        config['host'],
        config['port'],
        user=config['username'],
        password=config['password'],
    )
    session.open(False)
    check_session_validity1(session)
    session.close()
    session.open(False)
    check_session_validity1(session)
    session.close()


# 6、测试超时连接：需要手动测试，不清楚具体多久会超时
@pytest.mark.usefixtures('fixture_')
def test_session6():
   # open and read yaml files
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

   check_session_validity1(session)

   if config['enable_cluster']:
      # recovery session connection
      session_pool.put_back(session)
      # close session_pool connection
      session_pool.close()
   else:
      # close session connection
      session.close()

# 7、测试Session的SSL连接：需要手动测试，测试时请将use_ssl设置为True，并开启iotdb对应配置
def test_session_ssl_connection():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    session = Session(
        host=config['host'],
        port=config['port'],
        user=config['username'],
        password=config['password'],
        use_ssl=config['use_ssl'],
        ca_certs=config['ca_certs'],
    )
    session.open(False)
    check_session_validity1(session)
    session.close()

