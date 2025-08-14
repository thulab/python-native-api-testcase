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
        values_.append([True, i * -100, i * 100, i * -0.123, i * 0.123, "text" + str(i), "string" + str(i),
                        ("blob" + str(i)).encode('utf-8'), date(1970, 1, 1), i * 100])
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

        pool_config = PoolConfig(
            node_urls=config['node_urls'],
            user_name=config['username'],
            password=config['password'],
        )
        max_pool_size = 5
        wait_timeout_in_ms = 3000
        session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
        session = session_pool.get_session()
        session.open()
        session.execute_non_query_statement("DELETE DATABASE root.**")
        session.close()
        session_pool.close()
    except Exception as e:
        assert False, "There is an error:" + str(e)

    yield
    # 用例执行完成后清理环境代码

    try:
        # 读取配置文件
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)

        pool_config = PoolConfig(
            node_urls=config['node_urls'],
            user_name=config['username'],
            password=config['password'],
        )
        max_pool_size = 5
        wait_timeout_in_ms = 3000
        session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
        session = session_pool.get_session()
        session.open()
        session.execute_non_query_statement("DELETE DATABASE root.**")
        session.close()
        session_pool.close()
    except Exception as e:
        assert False, "There is an error:" + str(e)


# 1、测试session_pool：最简构造
@pytest.mark.usefixtures('fixture_')
def test_session_pool1():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    pool_config = PoolConfig(
        node_urls=config['node_urls'],
        user_name=config['username'],
        password=config['password'],
    )
    max_pool_size = 5
    wait_timeout_in_ms = 3000
    session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
    session = session_pool.get_session()
    session.open(False)
    check_session_validity1(session)
    session_pool.put_back(session)
    session_pool.close()


# 2、测试session_pool：最繁构造
@pytest.mark.usefixtures('fixture_')
def test_session_pool2():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    pool_config = PoolConfig(
        host=config['host'],
        port=config['port'],
        node_urls=config['node_urls'],
        user_name=config['username'],
        password=config['password'],
        fetch_size=1024,
        time_zone="Asia/Shanghai",
        max_retry=3,
        enable_compression=False,
        enable_redirection=True,
        use_ssl=False,
        ca_certs=None,
        connection_timeout_in_ms=None
    )
    max_pool_size = 5
    wait_timeout_in_ms = 3000
    session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
    session = session_pool.get_session()
    session.open(False)
    check_session_validity1(session)
    session_pool.put_back(session)
    session_pool.close()


# 3、测试SessionPool的SSL连接
def test_session_pool3():
    # open and read yaml files
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    pool_config = PoolConfig(
        node_urls=config['node_urls'],
        user_name=config['username'],
        password=config['password'],
        use_ssl=config['use_ssl'],
        ca_certs=config['ca_certs'],
    )
    max_pool_size = 5
    wait_timeout_in_ms = 3000
    session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
    session = session_pool.get_session()
    check_session_validity1(session)
    session_pool.put_back(session)
    session_pool.close()


# 4、测试session_pool：无node_urls
@pytest.mark.usefixtures('fixture_')
def test_session_pool4():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    pool_config = PoolConfig(
        host=config['host'],
        port=config['port'],
        user_name=config['username'],
        password=config['password'],
    )
    max_pool_size = 5
    wait_timeout_in_ms = 3000
    session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
    session = session_pool.get_session()
    session.open(False)
    check_session_validity1(session)
    session_pool.put_back(session)
    session_pool.close()

# 5、测试session_pool：push_back的session未open
def test_session_pool5():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    pool_config = PoolConfig(
        node_urls=config['node_urls'],
        user_name=config['username'],
        password=config['password'],
    )
    max_pool_size = 5
    wait_timeout_in_ms = 3000
    session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
    session = session_pool.get_session()
    session_pool.put_back(session)
    session_pool.close()

########### 异常情况 ##########
# 1、测试session_pool：node_urls为None
@pytest.mark.usefixtures('fixture_')
def test_session_pool_error1():
    try:
        PoolConfig(node_urls=None)
        assert False, "期望报错信息与实际不一致，期待：ValueError异常，实际无异常"
    except Exception as e:
        assert isinstance(e, ValueError), "期望报错信息与实际不一致，期待：ValueError异常，实际：" + str(e)


# 2、测试session_pool：get_session前关闭session_pool
def test_session_pool_error2():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    pool_config = PoolConfig(
        node_urls=config['node_urls'],
        user_name=config['username'],
        password=config['password'],
    )
    max_pool_size = 5
    wait_timeout_in_ms = 3000
    session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
    session_pool.close()
    try:
        session = session_pool.get_session()
        session.open(False)
        assert False, "期望报错信息与实际不一致，期待：ConnectionError异常，实际无异常"
    except Exception as e:
        assert isinstance(e, ConnectionError), "期望报错信息与实际不一致，期待：ConnectionError异常，实际：" + str(e)


# 3、测试session_pool：put_back的session_pool已经close
def test_session_pool_error3():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    pool_config = PoolConfig(
        node_urls=config['node_urls'],
        user_name=config['username'],
        password=config['password'],
    )
    max_pool_size = 5
    wait_timeout_in_ms = 3000
    session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
    session = session_pool.get_session()
    session_pool.close()
    try:
        session_pool.put_back(session)
        assert False, "期望报错信息与实际不一致，期待：ConnectionError异常，实际无异常"
    except Exception as e:
        assert isinstance(e, ConnectionError), "期望报错信息与实际不一致，期待：ConnectionError异常，实际：" + str(e)

# 4、测试session_pool：创建session_pool时，max_pool_size <= 0
@pytest.mark.usefixtures('fixture_')
def test_session_pool_error4():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    pool_config = PoolConfig(
        node_urls=config['node_urls'],
        user_name=config['username'],
        password=config['password'],
    )
    max_pool_size = -1
    wait_timeout_in_ms = 3000
    session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
    try:
        session_pool.get_session()
        assert False, "期望报错信息与实际不一致，期待：TimeoutError异常，实际无异常"
    except Exception as e:
        assert isinstance(e, TimeoutError), "期望报错信息与实际不一致，期待：TimeoutError异常，实际：" + str(e)
