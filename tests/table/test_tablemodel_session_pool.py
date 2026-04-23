import numpy as np
import pytest
import yaml
import os
import time
import uuid
from pathlib import Path
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

config_path = str(Path(__file__).resolve().parents[2] / "conf" / "config.yml")

# 读取配置文件
def read_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


def make_database_name(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def execute_with_retry(session, statement, retries=20, delay=1):
    last_error = None
    transient_parts = [
        "already exists",
        "doesn't exist",
        "does not exist",
        "is not exists",
        "Unknown database",
        "deleting database",
        "no available",
        "please retry",
        "Create SchemaPartition failed",
        "Create DataPartition failed",
        "operating table under the database",
    ]
    for _ in range(retries):
        try:
            return session.execute_non_query_statement(statement)
        except Exception as e:
            last_error = e
            if not any(part in str(e) for part in transient_parts):
                raise
            time.sleep(delay)
    raise last_error


def drop_database_if_exists(session, database_name, retries=20, delay=1):
    for _ in range(retries):
        try:
            session.execute_non_query_statement("use information_schema")
        except Exception:
            pass
        try:
            session.execute_non_query_statement("drop database " + database_name)
            time.sleep(delay)
            return
        except Exception as e:
            message = str(e)
            if "doesn't exist" in message or "Unknown database" in message:
                return
            time.sleep(delay)


def prepare_database(session, database_name, use_database=True):
    drop_database_if_exists(session, database_name)
    execute_with_retry(session, "create database " + database_name)
    for _ in range(20):
        with session.execute_query_statement("show databases") as session_data_set:
            names = []
            while session_data_set.has_next():
                names.append(str(session_data_set.next().get_fields()[0]))
        if database_name in names:
            break
        time.sleep(1)
    if use_database:
        execute_with_retry(session, "use " + database_name)


def insert_with_retry(session, tablet, retries=20, delay=1):
    last_error = None
    for _ in range(retries):
        try:
            session.insert(tablet)
            return
        except Exception as e:
            last_error = e
            message = str(e)
            if "305" not in message and "does not exist" not in message and "no available" not in message and "Create SchemaPartition failed" not in message:
                raise
            time.sleep(delay)
    raise last_error


def query_row_count_with_retry(session, table_name, retries=20, delay=1):
    last_error = None
    for _ in range(retries):
        try:
            actual = 0
            with session.execute_query_statement("select * from " + table_name + " order by time") as session_data_set:
                while session_data_set.has_next():
                    session_data_set.next()
                    actual = actual + 1
            return actual
        except Exception as e:
            last_error = e
            if "does not exist" not in str(e):
                raise
            time.sleep(delay)
    raise last_error


# 验证Session有效性
def check_session_validity1(session):
    expect = 0
    actual = 0
    database_name = make_database_name("test_tablemodel_session")
    table_name = "table1"
    prepare_database(session, database_name)
    execute_with_retry(session,
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

    time.sleep(1)
    tablet = Tablet(table_name, column_names, data_types, values, timestamps, column_types)
    insert_with_retry(session, tablet)
    actual = query_row_count_with_retry(session, table_name)

    assert expect == actual, "期待数量和实际数量不一致，期待：" + str(expect) + "，实际：" + str(actual)
    drop_database_if_exists(session, database_name)


# 验证Session有效性：session已经指定数据库
def check_session_validity2(session, database_name):
    expect = 0
    actual = 0
    table_name = "table1"
    execute_with_retry(session, "use " + database_name)
    execute_with_retry(session,
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

    time.sleep(1)
    tablet = Tablet(table_name, column_names, data_types, values, timestamps, column_types)
    insert_with_retry(session, tablet)
    actual = query_row_count_with_retry(session, table_name)

    assert expect == actual, "期待数量和实际数量不一致，期待：" + str(expect) + "，实际：" + str(actual)
    drop_database_if_exists(session, database_name)


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

    database_name = make_database_name("test_session2")
    bootstrap_config = TableSessionPoolConfig(
        node_urls=config['node_urls'],
        username=config['username'],
        password=config['password'],
    )
    bootstrap_pool = TableSessionPool(bootstrap_config)
    bootstrap_session = bootstrap_pool.get_session()
    prepare_database(bootstrap_session, database_name, use_database=False)
    bootstrap_session.close()
    bootstrap_pool.close()

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


