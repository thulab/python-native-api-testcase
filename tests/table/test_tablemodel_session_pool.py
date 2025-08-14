import numpy as np
import pytest
import yaml
import os
from datetime import date
from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.Session import Session
from iotdb.SessionPool import SessionPool, PoolConfig
from iotdb.table_session_pool import TableSessionPool, TableSessionPoolConfig
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet, ColumnType
from iotdb.utils.NumpyTablet import NumpyTablet
from datetime import date

from iotdb.utils.exception import IoTDBConnectionException

"""
 Title：测试表模型SessionPool连接接口
 Describe：测试SessionPool连接接口及其参数
 Author：肖林捷
 Date：2025/8/1
"""

config_path = "../conf/config.yml"

# 读取配置文件
def read_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


# 验证Session有效性
def check_session_validity1(session):
    expect = 0
    actual = 0
    database_name = "test_tablemodel_session"
    table_name = "table1"
    session.execute_non_query_statement("create database " + database_name)
    session.execute_non_query_statement("use " + database_name)
    session.execute_non_query_statement(
        "create table " + table_name + "("
                                       'id1 STRING TAG, id2 STRING TAG, id3 STRING TAG, '
                                       'attr1 string attribute, attr2 string attribute, attr3 string attribute,'
                                       'BOOLEAN BOOLEAN FIELD, INT32 INT32 FIELD, INT64 INT64 FIELD, FLOAT FLOAT FIELD, DOUBLE DOUBLE FIELD,'
                                       'TEXT TEXT FIELD, TIMESTAMP TIMESTAMP FIELD, DATE DATE FIELD, BLOB BLOB FIELD, STRING STRING FIELD)'
    )
    column_names = [
        "id1", "id2", "id3",
        "attr1", "attr2", "attr3",
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TIMESTAMP", "DATE", "BLOB", "STRING"]
    data_types = [
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
        TSDataType.TEXT,
        TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    column_types = [
        ColumnType.TAG, ColumnType.TAG, ColumnType.TAG,
        ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD]
    timestamps = []
    values = []
    for row in range(10):
        timestamps.append(row)
        values.append([
            "id1：" + str(row), "id2：" + str(row), "id3：" + str(row),
            "attr1:" + str(row), "attr2:" + str(row), "attr3:" + str(row),
            False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"])
        expect = expect + 1

    tablet = Tablet(table_name, column_names, data_types, values, timestamps, column_types)
    session.insert(tablet)
    with session.execute_query_statement("select * from " + table_name + " order by time") as session_data_set:
        while session_data_set.has_next():
            session_data_set.next()
            actual = actual + 1

    assert expect == actual, "期待数量和实际数量不一致，期待：" + str(expect) + "，实际：" + str(actual)
    session.execute_non_query_statement("drop database " + database_name)


# 验证Session有效性：session已经指定数据库
def check_session_validity2(session, database_name):
    expect = 0
    actual = 0
    table_name = "table1"
    session.execute_non_query_statement("create database " + database_name)
    # session.execute_non_query_statement("use " + database_name)
    session.execute_non_query_statement(
        "create table " + table_name + "("
                                       'id1 STRING TAG, id2 STRING TAG, id3 STRING TAG, '
                                       'attr1 string attribute, attr2 string attribute, attr3 string attribute,'
                                       'BOOLEAN BOOLEAN FIELD, INT32 INT32 FIELD, INT64 INT64 FIELD, FLOAT FLOAT FIELD, DOUBLE DOUBLE FIELD,'
                                       'TEXT TEXT FIELD, TIMESTAMP TIMESTAMP FIELD, DATE DATE FIELD, BLOB BLOB FIELD, STRING STRING FIELD)'
    )
    column_names = [
        "id1", "id2", "id3",
        "attr1", "attr2", "attr3",
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TIMESTAMP", "DATE", "BLOB", "STRING"]
    data_types = [
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
        TSDataType.TEXT,
        TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    column_types = [
        ColumnType.TAG, ColumnType.TAG, ColumnType.TAG,
        ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD]
    timestamps = []
    values = []
    for row in range(10):
        timestamps.append(row)
        values.append([
            "id1：" + str(row), "id2：" + str(row), "id3：" + str(row),
            "attr1:" + str(row), "attr2:" + str(row), "attr3:" + str(row),
            False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"])
        expect = expect + 1

    tablet = Tablet(table_name, column_names, data_types, values, timestamps, column_types)
    session.insert(tablet)
    with session.execute_query_statement("select * from " + table_name + " order by time") as session_data_set:
        while session_data_set.has_next():
            session_data_set.next()
            actual = actual + 1

    assert expect == actual, "期待数量和实际数量不一致，期待：" + str(expect) + "，实际：" + str(actual)
    session.execute_non_query_statement("drop database " + database_name)


# 测试 fixture：测试环境清理
@pytest.fixture()
def fixture_():
    yield
    # 用例执行完成后清理环境代码

    try:
        # 读取配置文件
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)

        config = TableSessionPoolConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
        )
        session_pool = TableSessionPool(config)
        session = session_pool.get_session()

        # 清理环境
        with session.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema":
                    session.execute_non_query_statement("drop database " + str(fields[0]))

        # 关闭 session 和 session_pool
        session.close()
        session_pool.close()
    except Exception as e:
        print(e)


# 1、测试最简 SessionPool 连接
@pytest.mark.usefixtures('fixture_')
def test_session1():
    config = TableSessionPoolConfig()
    session_pool = TableSessionPool(config)
    session = session_pool.get_session()
    check_session_validity1(session)
    session.close()
    session_pool.close()


# 2、测试最全 session 连接：session指定数据库
@pytest.mark.usefixtures('fixture_')
def test_session2():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    database_name = "test_session2"

    config = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        database=database_name,
        fetch_size=5000,
        time_zone=Session.DEFAULT_ZONE_ID,
        enable_redirection=True,
        enable_compression=False,
        use_ssl=False,
        ca_certs=None,
        connection_timeout_in_ms=None,
        max_pool_size=5,
        wait_timeout_in_ms=10000,
        max_retry=3
    )
    session_pool = TableSessionPool(config)
    session = session_pool.get_session()
    check_session_validity2(session, database_name)
    session.close()
    session_pool.close()


# 3、测试 enable_redirection 参数：关闭重定向
@pytest.mark.usefixtures('fixture_')
def test_session3():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    config = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        enable_redirection=False,
    )
    session_pool = TableSessionPool(config)
    session = session_pool.get_session()
    check_session_validity1(session)
    session.close()
    session_pool.close()


# 4、测试 enable_compression 参数：开启RPC压缩（需要单独测试，IoTDB需要开启对应配置）
@pytest.mark.usefixtures('fixture_')
def test_session4():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    config = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        enable_compression=config['enable_compression']
    )
    session_pool = TableSessionPool(config)
    session = session_pool.get_session()
    check_session_validity1(session)
    session.close()
    session_pool.close()


# 5、测试 use_ssl 参数：开启SSL连接（需要单独测试，生成并存放密钥）
@pytest.mark.usefixtures('fixture_')
def test_session5():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    config = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        use_ssl=config['use_ssl'],
        ca_certs=config['ca_certs']
    )
    session_pool = TableSessionPool(config)
    session = session_pool.get_session()
    check_session_validity1(session)
    session.close()
    session_pool.close()


# 6、测试 connection_timeout_in_ms 参数
@pytest.mark.usefixtures('fixture_')
def test_session6():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    config = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        connection_timeout_in_ms=config['connection_timeout_in_ms'],
    )
    try:
        session_pool = TableSessionPool(config)
        session = session_pool.get_session()
        check_session_validity1(session)
        session.close()
        session_pool.close()
    except Exception as e:
        assert isinstance(e, IoTDBConnectionException) and "read timeout" in str(
            e), "期望报错信息与实际不一致，期待：IoTDBConnectionException异常且包含'read timeout'信息，实际：" + str(e)

# # 7、测试多次 Close：session.close超过10次则会卡住
# @pytest.mark.usefixtures('fixture_')
# def test_session7():
#     # 读取配置文件
#     with open(config_path, 'r', encoding='utf-8') as file:
#         config = yaml.safe_load(file)
#
#     config = TableSessionPoolConfig(
#         node_urls=config['node_urls'],
#         username=config['username'],
#         password=config['password']
#     )
#     session_pool = TableSessionPool(config)
#     session = session_pool.get_session()
#     check_session_validity1(session)
#     # 关闭 session
#     for i in range(2):
#         session.close()
#
#     session_pool.close()

# 8、测试 max_retry 参数：0, 1, 100, 10000
@pytest.mark.usefixtures('fixture_')
def test_session8():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    config1 = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        max_retry=0
    )
    session_pool1 = TableSessionPool(config1)
    session1 = session_pool1.get_session()
    check_session_validity1(session1)
    session1.close()
    session_pool1.close()

    config2 = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        max_retry=1
    )
    session_pool2 = TableSessionPool(config2)
    session2 = session_pool2.get_session()
    check_session_validity1(session2)
    session2.close()
    session_pool2.close()

    config3 = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        max_retry=100
    )
    session_pool3 = TableSessionPool(config3)
    session3 = session_pool3.get_session()
    check_session_validity1(session3)
    session3.close()
    session_pool3.close()

    config4 = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        max_retry=10000
    )
    session_pool4 = TableSessionPool(config4)
    session4 = session_pool4.get_session()
    check_session_validity1(session4)
    session4.close()
    session_pool4.close()

# 9、测试 max_pool_size 参数：0, 1, 100, 10000
@pytest.mark.usefixtures('fixture_')
def test_session9():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    config1 = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        max_pool_size=0
    )
    try:
        session_pool1 = TableSessionPool(config1)
        session1 = session_pool1.get_session()
        check_session_validity1(session1)
        session1.close()
        session_pool1.close()
    except Exception as e:
        assert "Wait to get session timeout in SessionPool, current pool size: 0" in str(e), f"期望报错信息与实际不一致，期待包含: Wait to get session timeout in SessionPool, current pool size: 0, 实际: {str(e)}"

    config2 = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        max_pool_size=1
    )
    session_pool2 = TableSessionPool(config2)
    session2 = session_pool2.get_session()
    check_session_validity1(session2)
    session2.close()
    session_pool2.close()

    config3 = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        max_pool_size=100
    )
    session_pool3 = TableSessionPool(config3)
    session3 = session_pool3.get_session()
    check_session_validity1(session3)
    session3.close()
    session_pool3.close()

    config4 = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
        max_pool_size=10000
    )
    session_pool4 = TableSessionPool(config4)
    session4 = session_pool4.get_session()
    check_session_validity1(session4)
    session4.close()
    session_pool4.close()


