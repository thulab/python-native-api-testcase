import numpy as np
import pytest
import yaml
import os
from datetime import date
from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.Session import Session
from iotdb.SessionPool import SessionPool, PoolConfig
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet, ColumnType
from iotdb.utils.NumpyTablet import NumpyTablet
from datetime import date

from iotdb.utils.exception import IoTDBConnectionException

"""
 Title：测试表模型Session连接接口
 Describe：测试Session连接接口及其参数
 Author：肖林捷
 Date：2025/8/1
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

        config = TableSessionConfig(
            node_urls=[f"{config['host']}:{config['port']}"],
            username=config['username'],
            password=config['password'],
        )
        session = TableSession(config)

        # 清理环境
        with session.execute_query_statement("show databases", 0) as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema":
                    session.execute_non_query_statement("drop database " + str(fields[0]))

        # 关闭 session
        session.close()
    except Exception as e:
        print(e)


# 1、测试最简 session 连接：默认url地址为localhost:6667
@pytest.mark.usefixtures('fixture_')
def test_session1():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    # 判断配置文件是否为默认值
    if [f"{config['host']}:{config['port']}"] == ["127.0.0.1:6667"]:
        config = TableSessionConfig()
        session = TableSession(config)
        # 验证有效性
        check_session_validity1(session)
        # 关闭 session
        session.close()


# 2、测试最全 session 连接：session指定数据库
@pytest.mark.usefixtures('fixture_')
def test_session2():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    database_name = "test_session2"

    config = TableSessionConfig(
        node_urls=[f"{config['host']}:{config['port']}"],
        username=config['username'],
        password=config['password'],
        database=database_name,
        fetch_size=5000,
        time_zone="Asia/Shanghai",
        enable_redirection=True,
        enable_compression=False,
        use_ssl=False,
        ca_certs=None,
        connection_timeout_in_ms=None,
    )
    session = TableSession(config)
    # 验证有效性
    check_session_validity2(session, database_name)
    # 关闭 session
    session.close()


# 3、测试 enable_redirection 参数：关闭重定向
@pytest.mark.usefixtures('fixture_')
def test_session3():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    config = TableSessionConfig(
        node_urls=[f"{config['host']}:{config['port']}"],
        username=config['username'],
        password=config['password'],
        enable_redirection=False,
    )
    session = TableSession(config)
    # 验证有效性
    check_session_validity1(session)
    # 关闭 session
    session.close()


# 4、测试 enable_compression 参数：开启RPC压缩（需要单独测试，IoTDB需要开启对应配置）
@pytest.mark.usefixtures('fixture_')
def test_session4():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    config = TableSessionConfig(
        node_urls=[f"{config['host']}:{config['port']}"],
        username=config['username'],
        password=config['password'],
        enable_compression=config['enable_compression']
    )
    session = TableSession(config)
    # 验证有效性
    check_session_validity1(session)
    # 关闭 session
    session.close()


# 5、测试 use_ssl 参数：开启SSL连接（需要单独测试，生成并存放密钥）
@pytest.mark.usefixtures('fixture_')
def test_session5():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    config = TableSessionConfig(
        node_urls=[f"{config['host']}:{config['port']}"],
        username=config['username'],
        password=config['password'],
        use_ssl=config['use_ssl'],
        ca_certs=config['ca_certs']
    )
    session = TableSession(config)
    # 验证有效性
    check_session_validity1(session)
    # 关闭 session
    session.close()


# 6、测试 connection_timeout_in_ms 参数
@pytest.mark.usefixtures('fixture_')
def test_session6():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    config = TableSessionConfig(
        node_urls=[f"{config['host']}:{config['port']}"],
        username=config['username'],
        password=config['password'],
        connection_timeout_in_ms=config['connection_timeout_in_ms'],
    )
    try:
        session = TableSession(config)
        # 验证有效性
        check_session_validity1(session)
        # 关闭 session
        session.close()
    except Exception as e:
        assert isinstance(e, IoTDBConnectionException) and "read timeout" in str(
            e), "期望报错信息与实际不一致，期待：IoTDBConnectionException异常且包含'read timeout'信息，实际：" + str(e)

# 7、测试多次 Close
@pytest.mark.usefixtures('fixture_')
def test_session7():
    # 读取配置文件
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    config = TableSessionConfig(
        node_urls=[f"{config['host']}:{config['port']}"],
        username=config['username'],
        password=config['password']
    )
    session = TableSession(config)
    # 验证有效性
    check_session_validity1(session)
    # 关闭 session
    for i in range(100):
        session.close()

################## 异常情况 ##################

# 1、IoTDBConnectionException异常：未实现
@pytest.mark.usefixtures('fixture_')
def test_session_error1():
    config = TableSessionConfig(
        node_urls=[f"192.0.2.0:6667"],
    )
    try:
        session = TableSession(config)
        session.execute_query_statement("SHOW DATABASES")
        assert False, "期望报错信息与实际不一致，期待：IoTDBConnectionException异常，实际无异常"
    except Exception as e:
        assert isinstance(e, IoTDBConnectionException), "期望报错信息与实际不一致，期待：IoTDBConnectionException异常，实际：" + str(e)

