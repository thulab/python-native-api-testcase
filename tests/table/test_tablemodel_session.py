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

    database_name = make_database_name("test_session2")
    bootstrap = TableSession(
        TableSessionConfig(
            node_urls=[f"{config['host']}:{config['port']}"],
            username=config['username'],
            password=config['password'],
        )
    )
    prepare_database(bootstrap, database_name, use_database=False)
    bootstrap.close()

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

# ===== merged from test_utils_exception.py =====
from types import SimpleNamespace

from iotdb.thrift.common.ttypes import TEndPoint, TSStatus

from iotdb.utils.exception import (
    IoTDBConnectionException,
    RedirectException,
    StatementExecutionException,
)


def test_exception_constructors_cover_all_paths():
    assert str(IoTDBConnectionException(reason="oops")) == "oops"
    assert str(IoTDBConnectionException(cause="cause")) == "cause"
    assert str(IoTDBConnectionException()) == ""

    status = TSStatus(code=500, message="failed")
    assert str(StatementExecutionException(status=status)) == "500: failed"
    assert str(StatementExecutionException(message="bad")) == "bad"

    endpoint = TEndPoint("127.0.0.1", 6667)
    redir = RedirectException(endpoint)
    assert redir.redirect_node == endpoint

    mapping = {"d1": SimpleNamespace(host="127.0.0.2", port=6668)}
    redir = RedirectException(mapping)
    assert redir.device_to_endpoint == mapping

# ===== merged from test_utils_rpc_utils.py =====
from types import SimpleNamespace

import pandas as pd
import pytest

from iotdb.utils.exception import RedirectException, StatementExecutionException
from iotdb.utils.rpc_utils import (
    convert_to_timestamp,
    isoformat,
    verify_success,
    verify_success_by_list,
    verify_success_with_redirection,
    verify_success_with_redirection_for_multi_devices,
)


def status(code, message="ok", sub_status=None, redirect_node=None):
    return SimpleNamespace(
        code=code,
        message=message,
        subStatus=sub_status,
        redirectNode=redirect_node,
    )


def test_verify_success_and_redirection_paths():
    assert verify_success(status(200)) == 0
    assert verify_success(status(400)) == 0
    assert verify_success_by_list([status(200), status(400)]) is None

    with pytest.raises(StatementExecutionException):
        verify_success(status(500, "boom"))

    with pytest.raises(StatementExecutionException) as exc_info:
        verify_success(status(302, "multi", [status(200), status(500, "bad")]))
    assert "bad" in str(exc_info.value)

    with pytest.raises(RedirectException):
        verify_success_with_redirection(status(200, redirect_node=SimpleNamespace()))

    with pytest.raises(RedirectException) as exc_info:
        verify_success_with_redirection_for_multi_devices(
            status(
                302,
                sub_status=[
                    status(200),
                    status(200, redirect_node=SimpleNamespace(host="127.0.0.1", port=6667)),
                ],
            ),
            ["d1", "d2"],
        )
    assert "d2" in str(exc_info.value.device_to_endpoint)


def test_convert_to_timestamp_and_isoformat_fallback(monkeypatch):
    original_timestamp = pd.Timestamp
    calls = {"count": 0}

    def fake_timestamp(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise ValueError("bad timezone")
        return original_timestamp(*args, **kwargs)

    monkeypatch.setattr("iotdb.utils.rpc_utils.pd.Timestamp", fake_timestamp)
    monkeypatch.setattr("iotdb.utils.rpc_utils.get_localzone_name", lambda: "UTC")

    ts = convert_to_timestamp(1000, "ms", "Bad/Zone")
    assert calls["count"] == 2
    assert ts.year == 1970
    assert ts.second == 1

    assert isoformat(ts, "ms").endswith("+00:00")
    with pytest.raises(ValueError):
        isoformat(ts, "seconds")


def test_convert_to_timestamp_out_of_bounds_and_isoformat_old_pandas(monkeypatch):
    from pandas._libs import OutOfBoundsDatetime

    original_timestamp = pd.Timestamp
    calls = {"count": 0}

    def fake_timestamp(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OutOfBoundsDatetime("too big")
        return original_timestamp(*args, **kwargs)

    monkeypatch.setattr("iotdb.utils.rpc_utils.pd.Timestamp", fake_timestamp)

    ts = convert_to_timestamp(1000, "ms", "UTC")
    assert calls["count"] == 2
    assert ts.year == 1970

    class FakeTs:
        def __init__(self):
            self.calls = 0

        def isoformat(self, timespec=None):
            self.calls += 1
            if self.calls == 1:
                raise ValueError("old pandas")
            return "fallback-ok"

    assert isoformat(FakeTs(), "ms") == "fallback-ok"

