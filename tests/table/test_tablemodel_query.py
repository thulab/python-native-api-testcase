import datetime
import pytest
import yaml
import logging
from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.table_session_pool import TableSessionPoolConfig, TableSessionPool
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet, ColumnType
from datetime import date
from iotdb.utils.Field import Field
from iotdb.utils.exception import StatementExecutionException

"""
 Title：测试表模型查询接口
 Describe：主要测试SessionDateSet类，包括各种函数、Field查询等
 Author：肖林捷
 Date：2025/1/7
"""

# 全局变量
config_path = "../conf/config.yml"
database_name = "test_query"
column_names = []
data_types = []
column_types = []
timestamps = []
values = []
session = None
session_pool = None


def read_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


def get_session_():
    global session
    global session_pool
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    if config['enable_cluster']:
        config = TableSessionConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
        )
        session = TableSession(config)
        return session
    else:
        config = TableSessionPoolConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
        )
        session_pool = TableSessionPool(config)
        session = session_pool.get_session()
        return session


def create_database(session_):
    # 创建数据库
    session_.execute_non_query_statement("create database " + database_name)
    session_.execute_non_query_statement("use " + database_name)


def create_table(session_):
    session_.execute_non_query_statement(
        "create table table_b("
        'id1 STRING TAG, id2 STRING TAG, id3 STRING TAG, '
        'attr1 string attribute, attr2 string attribute, attr3 string attribute,'
        'BOOLEAN BOOLEAN FIELD, INT32 INT32 FIELD, INT64 INT64 FIELD, FLOAT FLOAT FIELD, DOUBLE DOUBLE FIELD,'
        'TEXT TEXT FIELD, TIMESTAMP TIMESTAMP FIELD, DATE DATE FIELD, BLOB BLOB FIELD, STRING STRING FIELD)'
    )


def insert_data(session_):
    global column_names, data_types, column_types, timestamps, values
    # 1、一般写入
    table_name = "table_b"
    column_names = [
        "id1", "id2", "id3",
        "attr1", "attr2", "attr3",
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TIMESTAMP", "DATE", "BLOB", "STRING"]
    data_types = [
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
        TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    column_types = [
        ColumnType.TAG, ColumnType.TAG, ColumnType.TAG,
        ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD]
    timestamps = [-9223372036854775808, -2147483648, -10000, -10, -1, 0, 1, 214748364, 2147483647, 9223372036854775807]
    values = [
        [
            "id1：1", "id2：1", "id3：1",
            "attr1:1", "attr2:1", "attr3:1",
            False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"
        ],
        [
            "id1：2", "id2：2", "id3：2",
            "attr1:2", "attr2:2", "attr3:2",
            True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz",
            -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
            "abcdefghijklmnopqrstuvwsyz"
        ],
        [
            "id1：3", "id2：3", "id3：3",
            "attr1:3", "attr2:3", "attr3:3",
            True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
            9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
            "!@#$%^&*()_+}{|:`~-=[];,./<>?~"
        ],
        [
            "id1：4", "id2：4", "id3：4",
            "attr1:4", "attr2:4", "attr3:4",
            True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"
        ],
        [
            "id1：5", "id2：5", "id3：5",
            "attr1:5", "attr2:5", "attr3:5",
            True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1),
            '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"
        ],
        [
            "id1：6", "id2：6", "id3：6",
            "attr1:6", "attr2:6", "attr3:6",
            True, 10, 11, 4.123456, 4.123456789012345,
            "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11,
            date(1970, 1, 1),
            '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                'utf-8'),
            "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"
        ],
        [
            "id1：7", "id2：7", "id3：7",
            "attr1:7", "attr2:7", "attr3:7",
            True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
            'Hello, World!'.encode('utf-8'),
            "string01"
        ],
        [
            "id1：8", "id2：8", "id3：8",
            "attr1:8", "attr2:8", "attr3:8",
            None, None, None, None, None, None, None, None, None, None
        ],
        [
            None, None, None,
            None, None, None,
            True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "
        ],
        [
            None, None, None,
            None, None, None,
            None, None, None, None, None, None, None, None, None, None
        ]
    ]
    tablet = Tablet(table_name, column_names, data_types, values, timestamps, column_types)
    session_.insert(tablet)


# 准备指定数量且特定表名的数据
def read_data(session_, table_name, num):
    # 准备元数据
    session_.execute_non_query_statement("create database " + database_name)
    session_.execute_non_query_statement("use " + database_name)
    session_.execute_non_query_statement(
        "create table " + table_name + "("
                                       'id1 STRING TAG, id2 STRING TAG, id3 STRING TAG, '
                                       'attr1 string attribute, attr2 string attribute, attr3 string attribute,'
                                       'BOOLEAN BOOLEAN FIELD, INT32 INT32 FIELD, INT64 INT64 FIELD, FLOAT FLOAT FIELD, DOUBLE DOUBLE FIELD,'
                                       'TEXT TEXT FIELD, TIMESTAMP TIMESTAMP FIELD, DATE DATE FIELD, BLOB BLOB FIELD, STRING STRING FIELD)'
    )
    # 写入数据
    column_names = [
        "id1", "id2", "id3",
        "attr1", "attr2", "attr3",
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TIMESTAMP", "DATE", "BLOB", "STRING"]
    data_types = [
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
        TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    column_types = [
        ColumnType.TAG, ColumnType.TAG, ColumnType.TAG,
        ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD]
    timestamps = list(range(1, num + 1))
    values = [
        [
            f"id1：{i}", f"id2：{i}", f"id3：{i}",
            f"attr1:{i}", f"attr2:{i}", f"attr3:{i}",
            i % 2 == 0, i, i * 1000, i * 1.0, i * 1.0, f"text{i}", i * 1000, date(1970, 1, 1),
            f'text{i}'.encode('utf-8'), f"string{i}"
        ] for i in range(1, num - 3 + 1)
    ]
    values.append(
        [
            "id1：98", "id2：98", "id3：98",
            "attr1:98", "attr2:98", "attr3:98",
            None, None, None, None, None, None, None, None, None, None
        ])
    values.append(
        [
            None, None, None,
            None, None, None,
            True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "
        ]
    )
    values.append(
        [
            None, None, None,
            None, None, None,
            None, None, None, None, None, None, None, None, None, None
        ])
    tablet = Tablet(table_name, column_names, data_types, values, timestamps, column_types)
    session_.insert(tablet)


@pytest.fixture()
def fixture_():
    # 用例执行前的环境搭建代码
    global session
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    # 获取session
    session = get_session_()
    # 清理残余数据库
    try:
        with session.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema" and str(fields[0]) != "__audit":
                    session.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)
    # 创建数据库
    create_database(session)
    # 创建表
    create_table(session)
    # 写入数据
    insert_data(session)

    yield

    # 用例执行完成后清理环境代码
    column_names.clear()
    data_types.clear()
    column_types.clear()
    timestamps.clear()
    values.clear()
    # 清理数据库
    try:
        with session.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema" and str(fields[0]) != "__audit":
                    session.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)

    # 关闭 session
    if config['enable_cluster']:
        session.close()
    else:
        session.close()
        session_pool.close()


# 测试SessionDataSet：验证列类型和数据类型以及行数的正确性
@pytest.mark.usefixtures('fixture_')
def test_query1():
    expect = 10
    actual = 0
    with session.execute_query_statement(
            "select * from table_b order by time") as session_data_set:
        for index in range(len(column_names)):
            assert session_data_set.get_column_names()[index + 1] == column_names[
                index].lower(), "Column name does not match, expected:" + column_names[index] + ", actual:" + \
                                session_data_set.get_column_names()[index + 1]
        for index in range(len(data_types)):
            assert session_data_set.get_column_types()[index + 1] == data_types[
                index], "Column type does not match, expected:" + str(data_types[index]) + ", actual:" + str(
                session_data_set.get_column_types()[index + 1])
        while session_data_set.has_next():
            actual = actual + 1
            session_data_set.next()

        assert expect == actual, "Actual number of rows does not match, expected:" + str(expect) + ", actual:" + str(
            actual)
        # 会自动关闭
        session_data_set.close_operation_handle()


# 测试查询分批返回 DataFrame 功能：当 SessionDataSet 总行数超过 fetchSize 时
def test_query2():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    row_num = 1000
    fetch_size = 100

    if config['enable_cluster']:
        config = TableSessionConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
            fetch_size=fetch_size,
        )
        session_test_query2 = TableSession(config)
    else:
        config = TableSessionPoolConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
            fetch_size=fetch_size,
        )
        session_pool_test_query2 = TableSessionPool(config)
        session_test_query2 = session_pool_test_query2.get_session()

    # 清理残余数据库
    try:
        with session_test_query2.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema" and str(fields[0]) != "__audit":
                    session_test_query2.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)

    # 准备数据
    read_data(session_test_query2, "table_test_query2", row_num)

    # 测试 has_next_df 和 next_df
    with session_test_query2.execute_query_statement(
            "select * from table_test_query2 order by time") as session_data_set:
        while session_data_set.has_next_df():
            df = session_data_set.next_df()
            assert df.shape[0] == fetch_size, f"Expected {fetch_size} rows, but got {df.shape[0]} rows"

        # 会自动关闭
        session_data_set.close_operation_handle()

    # 清理残余数据库
    try:
        with session_test_query2.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema" and str(fields[0]) != "__audit":
                    session_test_query2.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)

# 测试查询分批返回 DataFrame 功能：当 SessionDataSet 剩余的行数小于 fetchSize 时
def test_query3():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    row_num = 30
    fetch_size = 1000

    if config['enable_cluster']:
        config = TableSessionConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
            fetch_size=fetch_size,
        )
        session_test_query2 = TableSession(config)
    else:
        config = TableSessionPoolConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
            fetch_size=fetch_size,
        )
        session_pool_test_query2 = TableSessionPool(config)
        session_test_query2 = session_pool_test_query2.get_session()

    # 清理残余数据库
    try:
        with session_test_query2.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema" and str(fields[0]) != "__audit":
                    session_test_query2.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)

    # 准备数据
    read_data(session_test_query2, "table_test_query2", row_num)

    # 测试 has_next_df 和 next_df
    with session_test_query2.execute_query_statement(
            "select * from table_test_query2 order by time") as session_data_set:
        while session_data_set.has_next_df():
            df = session_data_set.next_df()
            assert df.shape[0] == row_num, f"Expected {row_num} rows, but got {df.shape[0]} rows"

        # 会自动关闭
        session_data_set.close_operation_handle()

    # 清理残余数据库
    try:
        with session_test_query2.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema" and str(fields[0]) != "__audit":
                    session_test_query2.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)

# 测试查询分批返回 DataFrame 功能：当 SessionDataSet 剩余的行数等于 fetchSize 时
def test_query4():
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    row_num = 1000

    if config['enable_cluster']:
        config = TableSessionConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
            fetch_size=row_num,
        )
        session_test_query2 = TableSession(config)
    else:
        config = TableSessionPoolConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
            fetch_size=row_num,
        )
        session_pool_test_query2 = TableSessionPool(config)
        session_test_query2 = session_pool_test_query2.get_session()

    # 清理残余数据库
    try:
        with session_test_query2.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema" and str(fields[0]) != "__audit":
                    session_test_query2.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)

    # 准备数据
    read_data(session_test_query2, "table_test_query2", row_num)

    # 测试 has_next_df 和 next_df
    with session_test_query2.execute_query_statement(
            "select * from table_test_query2 order by time") as session_data_set:
        while session_data_set.has_next_df():
            df = session_data_set.next_df()
            assert df.shape[0] == row_num, f"Expected {row_num} rows, but got {df.shape[0]} rows"

        # 会自动关闭
        session_data_set.close_operation_handle()

    # 清理残余数据库
    try:
        with session_test_query2.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema" and str(fields[0]) != "__audit":
                    session_test_query2.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)

# 测试 fetchSize <= 0 的异常：Session 打印一个警告日志 ，fetchSize 重置为默认值 5000
def test_query5():
    # 设置日志捕获
    log_capture = []
    handler = logging.StreamHandler()
    handler.setLevel(logging.WARNING)
    logger = logging.getLogger("IoTDB")
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)

    def log_capture_func(record):
        log_capture.append(record.getMessage())

    handler.emit = log_capture_func
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    row_num = 10000

    if config['enable_cluster']:
        config = TableSessionConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
            fetch_size=0,
        )
        session_test_query2 = TableSession(config)
    else:
        config = TableSessionPoolConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
            fetch_size=0,
        )
        session_pool_test_query2 = TableSessionPool(config)
        session_test_query2 = session_pool_test_query2.get_session()

    # 验证日志是否被捕获
    assert any("fetch_size 0 is illegal" in log for log in log_capture), "Expected warning log not found"

    # 清理残余数据库
    try:
        with session_test_query2.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema" and str(fields[0]) != "__audit":
                    session_test_query2.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)

    # 准备数据
    read_data(session_test_query2, "table_test_query2", row_num)

    # 测试 has_next_df 和 next_df
    with session_test_query2.execute_query_statement(
            "select * from table_test_query2 order by time") as session_data_set:
        while session_data_set.has_next_df():
            df = session_data_set.next_df()
            assert df.shape[0] == 5000, f"Expected {5000} rows, but got {df.shape[0]} rows"

        # 会自动关闭
        session_data_set.close_operation_handle()

    # 清理残余数据库
    try:
        with session_test_query2.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema" and str(fields[0]) != "__audit":
                    session_test_query2.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)



# 测试fields查询：验证类型
@pytest.mark.usefixtures('fixture_')
def test_fields1():
    with session.execute_query_statement(
            "select * from table_b order by time") as session_data_set:
        while session_data_set.has_next():
            row_record = session_data_set.next()
            # time列
            assert row_record.get_fields()[0].get_data_type() == 8
            # 其他列
            for col_index in range(len(data_types)):
                assert row_record.get_fields()[col_index + 1].get_data_type() == data_types[
                    col_index], "Actual type does not match, expected:" + str(
                    data_types[col_index]) + ", actual:" + str(
                    row_record.get_fields()[col_index + 1].get_data_type())


# 测试fields查询：验证字段值
@pytest.mark.usefixtures('fixture_')
def test_fields2():
    row_index = 0
    with session.execute_query_statement(
            "select * from table_b order by time") as session_data_set:
        while session_data_set.has_next():
            row_record = session_data_set.next()
            # 1、测试time列的值
            if 2147483647 <= timestamps[row_index] or timestamps[
                row_index] <= 0:  # TODO：待优化，fromtimestamp方法无法处理超过一定范围的时间戳
                # 对于特殊时间戳值，只验证不为None
                assert row_record.get_fields()[0].get_object_value(TSDataType.TIMESTAMP) is not None, \
                    "Actual time does not match, expected:" + str(timestamps[row_index]) + ", actual:" + str()
            else:
                # 其他时间戳正常处理（包括get_object_value和get_timestamp_value）
                expected_datetime = datetime.datetime.fromtimestamp(timestamps[row_index] / 1000.0)
                assert row_record.get_fields()[0].get_object_value(TSDataType.TIMESTAMP).strftime(
                    '%Y-%m-%d %H:%M:%S') == expected_datetime.strftime(
                    '%Y-%m-%d %H:%M:%S'), \
                    "Actual value does not match, expected:" + str(expected_value) + ", actual:" + str(
                        actual_value)
                assert row_record.get_fields()[0].get_timestamp_value().strftime(
                    '%Y-%m-%d %H:%M:%S') == expected_datetime.strftime(
                    '%Y-%m-%d %H:%M:%S'), \
                    "Actual value does not match, expected:" + str(expected_value) + ", actual:" + str(
                        actual_value)
            # 2、测试其他列的值（TAG列、属性列和Filed列）
            for col_index in range(len(values[row_index])):
                # 测试get_object_value函数
                if data_types[col_index] == TSDataType.TIMESTAMP:  # timestamp类型返回值需要特殊处理
                    expected_value = values[row_index][col_index]
                    actual_value = row_record.get_fields()[col_index + 1].get_object_value(data_types[col_index])
                    if expected_value is not None:
                        # 处理特殊时间戳值
                        if 2147483647 <= expected_value or expected_value <= 0:  # TODO：待优化，fromtimestamp方法无法处理超过一定范围的时间戳
                            # 对于特殊时间戳值，只验证不为None
                            assert actual_value is not None, \
                                "Actual value should not be None for special timestamp, expected:" + str(expected_value)
                        else:
                            # 其他时间戳正常处理
                            expected_datetime = datetime.datetime.fromtimestamp(expected_value / 1000.0)
                            assert actual_value.strftime('%Y-%m-%d %H:%M:%S') == expected_datetime.strftime(
                                '%Y-%m-%d %H:%M:%S'), \
                                "Actual value does not match, expected:" + str(expected_value) + ", actual:" + str(
                                    actual_value)
                    else:
                        assert actual_value is None, "Actual value does not match, expected: None, actual:" + str(
                            actual_value)
                elif data_types[col_index] == TSDataType.FLOAT:  # float类型返回值需要特殊处理：会存在浮点数精度问题
                    expected_value = values[row_index][col_index]
                    actual_value = row_record.get_fields()[col_index + 1].get_object_value(data_types[col_index])
                    if expected_value is not None:
                        assert abs(actual_value - expected_value) < 1e-6, \
                            "Actual value does not match, expected:" + str(expected_value) + ", actual:" + str(
                                actual_value)
                    else:
                        assert actual_value is None, "Actual value does not match, expected: None, actual:" + str(
                            actual_value)
                elif data_types[col_index] == TSDataType.DOUBLE:  # double类型返回值需要特殊处理：会存在浮点数精度问题
                    expected_value = values[row_index][col_index]
                    actual_value = row_record.get_fields()[col_index + 1].get_object_value(data_types[col_index])
                    if expected_value is not None:
                        assert abs(actual_value - expected_value) < 1e-12, \
                            "Actual value does not match, expected:" + str(expected_value) + ", actual:" + str(
                                actual_value)
                    else:
                        assert actual_value is None, "Actual value does not match, expected: None, actual:" + str(
                            actual_value)
                else:  # 其他类型的原有比较逻辑保持不变
                    assert row_record.get_fields()[col_index + 1].get_object_value(data_types[col_index]) == \
                           values[row_index][
                               col_index], "Actual value does not match, expected:" + str(
                        values[row_index][col_index]) + ", actual:" + str(
                        row_record.get_fields()[col_index + 1].get_object_value(data_types[col_index]))

                # 3、测试其他get_xxx_value函数
                if data_types[col_index] == TSDataType.BOOLEAN:
                    assert row_record.get_fields()[col_index + 1].get_bool_value() == values[row_index][
                        col_index], "Actual value does not match, expected:" + str(
                        values[row_index][col_index]) + ", actual:" + str(
                        row_record.get_fields()[col_index + 1].get_bool_value())
                elif data_types[col_index] == TSDataType.INT32:
                    assert row_record.get_fields()[col_index + 1].get_int_value() == values[row_index][
                        col_index], "Actual value does not match, expected:" + str(
                        values[row_index][col_index]) + ", actual:" + str(
                        row_record.get_fields()[col_index + 1].get_int_value())
                elif data_types[col_index] == TSDataType.INT64:
                    assert row_record.get_fields()[col_index + 1].get_long_value() == values[row_index][
                        col_index], "Actual value does not match, expected:" + str(
                        values[row_index][col_index]) + ", actual:" + str(
                        row_record.get_fields()[col_index + 1].get_long_value())
                elif data_types[col_index] == TSDataType.FLOAT:
                    expected_value = values[row_index][col_index]
                    actual_value = row_record.get_fields()[col_index + 1].get_float_value()
                    if expected_value is not None:
                        assert abs(actual_value - expected_value) < 1e-6, \
                            "Actual value does not match, expected:" + str(expected_value) + ", actual:" + str(
                                actual_value)
                    else:
                        assert actual_value is None, "Actual value does not match, expected: None, actual:" + str(
                            actual_value)
                elif data_types[col_index] == TSDataType.DOUBLE:
                    assert row_record.get_fields()[col_index + 1].get_double_value() == values[row_index][
                        col_index], "Actual value does not match, expected:" + str(
                        values[row_index][col_index]) + ", actual:" + str(
                        row_record.get_fields()[col_index + 1].get_double_value())
                elif data_types[col_index] == TSDataType.TEXT:
                    assert row_record.get_fields()[col_index + 1].get_string_value() == (
                        "None" if values[row_index][col_index] is None else values[row_index][
                            col_index]), "Actual value does not match, expected:" + str(
                        values[row_index][col_index]) + ", actual:" + str(
                        row_record.get_fields()[col_index + 1].get_string_value())
                elif data_types[col_index] == TSDataType.DATE:
                    assert row_record.get_fields()[col_index + 1].get_date_value() == values[row_index][
                        col_index], "Actual value does not match, expected:" + str(
                        values[row_index][col_index]) + ", actual:" + str(
                        row_record.get_fields()[col_index + 1].get_date_value())
                elif data_types[col_index] == TSDataType.STRING:
                    assert row_record.get_fields()[col_index + 1].get_string_value() == (
                        "None" if values[row_index][col_index] is None else values[row_index][
                            col_index]), "Actual value does not match, expected:" + str(
                        values[row_index][col_index]) + ", actual:" + str(
                        row_record.get_fields()[col_index + 1].get_string_value())
                elif data_types[col_index] == TSDataType.BLOB:
                    assert row_record.get_fields()[col_index + 1].get_binary_value() == (
                        None if values[row_index][col_index] is None else values[row_index][
                            col_index]), "Actual value does not match, expected:" + str(
                        values[row_index][col_index]) + ", actual:" + str(
                        row_record.get_fields()[col_index + 1].get_binary_value())
                elif data_types[col_index] == TSDataType.TIMESTAMP:
                    expected_value = values[row_index][col_index]
                    actual_value = row_record.get_fields()[col_index + 1].get_timestamp_value()
                    if expected_value is not None:
                        # 处理特殊时间戳值
                        if expected_value in [0, -9223372036854775808, 9223372036854775807]:
                            # 对于特殊时间戳值，只验证不为None
                            assert actual_value is not None, \
                                "Actual value should not be None for special timestamp, expected:" + str(expected_value)
                        else:
                            # 其他时间戳正常处理
                            try:
                                expected_datetime = datetime.datetime.fromtimestamp(expected_value / 1000.0)
                                assert actual_value.strftime('%Y-%m-%d %H:%M:%S') == expected_datetime.strftime(
                                    '%Y-%m-%d %H:%M:%S'), \
                                    "Actual value does not match, expected:" + str(expected_value) + ", actual:" + str(
                                        actual_value)
                            except (OSError, ValueError):
                                # 如果fromtimestamp失败，使用get_timestamp_value进行比较
                                assert actual_value == row_record.get_fields()[col_index + 1].get_timestamp_value(), \
                                    "Actual value does not match, expected:" + str(expected_value) + ", actual:" + str(
                                        actual_value)
                    else:
                        assert actual_value is None, "Actual value does not match, expected: None, actual:" + str(
                            actual_value)
            row_index = row_index + 1


# 测试fields查询：测试is_null、copy和set_xxx_value等方法有效性
@pytest.mark.usefixtures('fixture_')
def test_fields3():
    row_index = 0
    with session.execute_query_statement(
            "select * from table_b order by time") as session_data_set:
        while session_data_set.has_next():
            row_record = session_data_set.next()
            for col_index in range(len(values[row_index])):
                assert row_record.get_fields()[col_index + 1].is_null() == (
                        values[row_index][col_index] is None), "is_null does not match, expected value:" + str(
                    values[row_index][col_index]) + ", actual value:" + str(
                    row_record.get_fields()[col_index + 1].get_object_value(data_types[col_index])) + ", record:" + str(
                    row_record)

            # copy
            row_record.get_fields()[1].copy(row_record.get_fields()[1])
            row_record.get_fields()[6].copy(row_record.get_fields()[6])
            row_record.get_fields()[7].copy(row_record.get_fields()[7])
            row_record.get_fields()[8].copy(row_record.get_fields()[8])
            row_record.get_fields()[9].copy(row_record.get_fields()[9])
            row_record.get_fields()[10].copy(row_record.get_fields()[10])
            row_record.get_fields()[11].copy(row_record.get_fields()[11])
            row_record.get_fields()[12].copy(row_record.get_fields()[12])
            row_record.get_fields()[13].copy(row_record.get_fields()[13])
            row_record.get_fields()[14].copy(row_record.get_fields()[14])
            row_record.get_fields()[15].copy(row_record.get_fields()[15])

            # get_field
            Field.get_field(None, TSDataType.BOOLEAN)
            Field.get_field(True, TSDataType.BOOLEAN)

            # set_XXX_value
            row_record.get_fields()[6].set_bool_value(values[row_index][6])
            row_record.get_fields()[7].set_int_value(values[row_index][7])
            row_record.get_fields()[8].set_long_value(values[row_index][8])
            row_record.get_fields()[9].set_float_value(values[row_index][9])
            row_record.get_fields()[10].set_double_value(values[row_index][10])
            row_record.get_fields()[14].set_binary_value(values[row_index][14])

            row_index = row_index + 1


################## 异常情况 ##################

# 测试fields查询， get_XXX_valu e异常情况
@pytest.mark.usefixtures('fixture_')
def test_fields_error1():
    field = Field(None, None)
    try:
        field.get_bool_value()
        assert False, "期待报错，实际无报错"
    except Exception as e:
        assert isinstance(e, Exception) and str(
            e) == "Null Field Exception!", "期待报错信息与实际不一致，期待：Exception: Null Field Exception!，实际：" + type(
            e).__name__ + ":" + str(e)

    try:
        field.get_int_value()
        assert False, "期待报错，实际无报错"
    except Exception as e:
        assert isinstance(e, Exception) and str(
            e) == "Null Field Exception!", "期待报错信息与实际不一致，期待：Exception: Null Field Exception!，实际：" + type(
            e).__name__ + ":" + str(e)

    try:
        field.get_long_value()
        assert False, "期待报错，实际无报错"
    except Exception as e:
        assert isinstance(e, Exception) and str(
            e) == "Null Field Exception!", "期待报错信息与实际不一致，期待：Exception: Null Field Exception!，实际：" + type(
            e).__name__ + ":" + str(e)

    try:
        field.get_float_value()
        assert False, "期待报错，实际无报错"
    except Exception as e:
        assert isinstance(e, Exception) and str(
            e) == "Null Field Exception!", "期待报错信息与实际不一致，期待：Exception: Null Field Exception!，实际：" + type(
            e).__name__ + ":" + str(e)

    try:
        field.get_double_value()
        assert False, "期待报错，实际无报错"
    except Exception as e:
        assert isinstance(e, Exception) and str(
            e) == "Null Field Exception!", "期待报错信息与实际不一致，期待：Exception: Null Field Exception!，实际：" + type(
            e).__name__ + ":" + str(e)

    field.get_string_value()

    try:
        field.get_date_value()
        assert False, "期待报错，实际无报错"
    except Exception as e:
        assert isinstance(e, Exception) and str(
            e) == "Null Field Exception!", "期待报错信息与实际不一致，期待：Exception: Null Field Exception!，实际：" + type(
            e).__name__ + ":" + str(e)

    try:
        field.get_timestamp_value()
        assert False, "期待报错，实际无报错"
    except Exception as e:
        assert isinstance(e, Exception) and str(
            e) == "Null Field Exception!", "期待报错信息与实际不一致，期待：Exception: Null Field Exception!，实际：" + type(
            e).__name__ + ":" + str(e)

    try:
        field.get_binary_value()
        assert False, "期待报错，实际无报错"
    except Exception as e:
        assert isinstance(e, Exception) and str(
            e) == "Null Field Exception!", "期待报错信息与实际不一致，期待：Exception: Null Field Exception!，实际：" + type(
            e).__name__ + ":" + str(e)


# 测试 StatementExecutionException 异常
@pytest.mark.usefixtures('fixture_')
def test_query_error1():
    try:
        session.execute_query_statement("StatementExecutionException")
        assert False, "期待报错，实际无报错"
    except Exception as e:
        assert isinstance(e, StatementExecutionException) and "StatementExecutionException" in str(
            e), "期待报错信息与实际不一致，期待：StatementExecutionException: ，实际：" + type(e).__name__ + ":" + str(e)

# ===== merged from test_utils_field_rowrecord.py =====
import pandas as pd
import pytest

from iotdb.utils.Field import Field
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.RowRecord import RowRecord


class UnknownType:
    pass


def test_field_value_accessors_and_copy():
    ts = Field(TSDataType.TIMESTAMP, 1, timezone="UTC", precision="ms")
    date_field = Field(TSDataType.DATE, 20240417)
    object_field = Field(TSDataType.OBJECT, b"hello")

    assert Field.copy(Field(TSDataType.BOOLEAN, True)).get_bool_value() is True
    assert int(Field.copy(Field(TSDataType.INT32, 7)).get_int_value()) == 7
    assert int(Field.copy(Field(TSDataType.INT64, 8)).get_long_value()) == 8
    assert float(Field.copy(Field(TSDataType.FLOAT, 1.5)).get_float_value()) == pytest.approx(1.5)
    assert float(Field.copy(Field(TSDataType.DOUBLE, 2.5)).get_double_value()) == pytest.approx(2.5)
    assert Field.copy(Field(TSDataType.TEXT, b"abc")).get_string_value() == "abc"
    assert Field.copy(Field(TSDataType.STRING, b"xyz")).get_object_value(TSDataType.STRING) == "xyz"
    assert Field.copy(Field(TSDataType.BLOB, b"\x01\x02")).get_binary_value() == b"\x01\x02"
    assert object_field.get_string_value() == "hello"
    assert object_field.get_object_value(TSDataType.OBJECT) == "hello"
    assert ts.get_string_value().startswith("1970-01-01T00:00:00.001")
    assert date_field.get_date_value() is not None


def test_field_null_and_error_paths():
    assert Field.get_field(None, TSDataType.INT32) is None
    assert Field.get_field(pd.NA, TSDataType.INT32) is None

    null_field = Field(None)
    assert null_field.is_null()
    assert null_field.get_string_value() == "None"

    with pytest.raises(Exception):
        null_field.get_bool_value()

    with pytest.raises(RuntimeError):
        Field(TSDataType.OBJECT, b"payload").get_binary_value()

    with pytest.raises(RuntimeError):
        Field(TSDataType.INT32, 1).get_object_value(UnknownType())

    text_field = Field(TSDataType.TEXT, b"abc")
    assert text_field.get_bool_value() is None
    assert text_field.get_int_value() is None
    assert text_field.get_long_value() is None
    assert text_field.get_float_value() is None
    assert text_field.get_double_value() is None
    assert text_field.get_binary_value() == b"abc"
    assert text_field.get_timestamp_value() is None
    assert text_field.get_date_value() is None

    with pytest.raises(Exception):
        Field.copy(Field(UnknownType(), 1))


def test_row_record_str_and_mutation():
    record = RowRecord(
        100,
        [
            Field(TSDataType.INT32, 7),
            Field(TSDataType.TEXT, b"alpha"),
        ],
    )
    record.add_field(b"payload", TSDataType.OBJECT)
    record.set_field(0, Field(TSDataType.INT32, 9))
    record.set_timestamp(200)

    assert record.get_timestamp() == 200
    assert [field.get_string_value() for field in record.get_fields()] == [
        "9",
        "alpha",
        "payload",
    ]
    assert str(record) == "200\t\t9\t\talpha\t\tpayload"

# ===== merged from test_utils_iotdb_rpc_dataset.py =====
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.iotdb_rpc_dataset import IoTDBRpcDataSet


class FakeClient:
    def __init__(self, response):
        self.response = response
        self.close_requests = []
        self.fetch_requests = []

    def fetchResultsV2(self, request):
        self.fetch_requests.append(request)
        return self.response

    def closeOperation(self, request):
        self.close_requests.append(request)
        return SimpleNamespace(message="closed")


def fake_deserialize_factory():
    calls = {"count": 0}

    def fake_deserialize(_):
        calls["count"] += 1
        if calls["count"] == 1:
            return (
                np.array([1, 2], dtype=">i8"),
                [
                    np.array([True, False], dtype="?"),
                    np.array([1, 2], dtype=">i4"),
                    np.array([1.5, 2.5], dtype=">f4"),
                    np.array([b"aa", b"bb"], dtype=object),
                    np.array([1000, 2000], dtype=">i8"),
                    np.array([20240416, 20240417], dtype=">i4"),
                    np.array([b"x", b"y"], dtype=object),
                ],
                [None, None, None, None, None, None, None],
                2,
            )
        return (
            np.array([3, 4], dtype=">i8"),
                [
                    np.array([False, True], dtype="?"),
                    np.array([3, 4], dtype=">i4"),
                    np.array([3.5, 4.5], dtype=">f4"),
                    np.array([b"cc", b"dd"], dtype=object),
                    np.array([3000, 4000], dtype=">i8"),
                    np.array([20240418, 20240419], dtype=">i4"),
                    np.array([b"z", b"w"], dtype=object),
                ],
            [None, None, None, None, None, None, None],
            2,
        )

    return fake_deserialize


def build_dataset(monkeypatch, query_result, fetch_size=2, more_data=False, ignore_timestamp=False):
    monkeypatch.setattr("iotdb.utils.iotdb_rpc_dataset.deserialize", fake_deserialize_factory())
    return IoTDBRpcDataSet(
        "select * from t",
        ["flag", "count", "ratio", "txt", "ts", "day", "obj"],
        ["BOOLEAN", "INT32", "FLOAT", "TEXT", "TIMESTAMP", "DATE", "OBJECT"],
        ignore_timestamp,
        more_data,
        1,
        FakeClient(SimpleNamespace(code=200, message="ok", moreData=False, hasResultSet=True, queryResult=query_result)),
        2,
        3,
        query_result,
        fetch_size,
        10,
        "UTC",
        "ms",
        None,
    )


def test_dataset_next_and_dataframe_building(monkeypatch):
    dataset = build_dataset(monkeypatch, [b"chunk-1"], fetch_size=2)
    assert dataset.next() is True
    assert isinstance(dataset.data_frame, pd.DataFrame)
    assert list(dataset.data_frame.columns) == list(range(8))
    assert dataset.has_cached_result() is True

    dataset.construct_one_data_frame()
    assert dataset.has_cached_result() is True
    assert dataset.find_column_name_by_index(1) == "Time"
    with pytest.raises(Exception):
        dataset.find_column_name_by_index(0)
    with pytest.raises(Exception):
        dataset.find_column_name_by_index(99)


def test_dataset_next_dataframe_and_buffering(monkeypatch):
    dataset = build_dataset(monkeypatch, [b"chunk-1"], fetch_size=1)
    first = dataset.next_dataframe()
    second = dataset.next_dataframe()

    assert list(first["flag"]) == [True]
    assert list(second["flag"]) == [False]
    assert dataset._has_buffered_data() is False


def test_dataset_result_set_to_pandas_fetch_and_close(monkeypatch):
    response = SimpleNamespace(
        status=SimpleNamespace(code=200, message="ok", subStatus=None, redirectNode=None),
        moreData=False,
        hasResultSet=True,
        queryResult=[b"chunk-1"],
    )
    client = FakeClient(response)
    dataset = IoTDBRpcDataSet(
        "select * from t",
        ["flag"],
        ["BOOLEAN"],
        False,
        True,
        1,
        client,
        2,
        3,
        [b"chunk-1"],
        2,
        10,
        "UTC",
        "ms",
        None,
    )

    monkeypatch.setattr("iotdb.utils.iotdb_rpc_dataset.deserialize", fake_deserialize_factory())
    assert dataset.fetch_results() is True
    assert client.fetch_requests
    assert dataset.result_set_to_pandas().shape[0] == 2
    dataset.close()
    assert client.close_requests
    dataset.close()

# ===== merged from test_utils_session_dataset.py =====
from types import SimpleNamespace

import pandas as pd
import pytest

from iotdb.utils.Field import Field
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.RowRecord import RowRecord
from iotdb.utils.SessionDataSet import SessionDataSet, get_typed_point


class FakeRpcDataSet:
    def __init__(self, *args):
        self.ignore_timestamp = args[3]
        self.fetch_size = args[10]
        self.has_cached_data_frame = False
        self.data_frame = None
        self.closed = False
        self.buffered = False
        self.more_result = False
        self.column_names = ["obj"] if self.ignore_timestamp else ["Time", "obj"]
        self.column_types = [TSDataType.TEXT] if self.ignore_timestamp else [TSDataType.INT64, TSDataType.TEXT]
        self.next_dataframe_calls = 0

    def get_column_types(self):
        return self.column_types

    def get_column_names(self):
        return self.column_names

    def next(self):
        if self.data_frame is None:
            if self.ignore_timestamp:
                self.data_frame = pd.DataFrame({"obj": [b"alpha", b"beta"]})
            else:
                self.data_frame = pd.DataFrame({"Time": [100, 200], "obj": [b"alpha", b"beta"]})
        self.has_cached_data_frame = True
        return True

    def close(self):
        self.closed = True

    def _has_buffered_data(self):
        return self.buffered

    def _has_next_result_set(self):
        return self.more_result

    def next_dataframe(self):
        self.next_dataframe_calls += 1
        return pd.DataFrame({"obj": [b"chunk-1", b"chunk-2"]})

    def result_set_to_pandas(self):
        return pd.DataFrame({"obj": [b"alpha"]})

    def get_fetch_size(self):
        return self.fetch_size

    def set_fetch_size(self, fetch_size):
        self.fetch_size = fetch_size


def build_session(monkeypatch, ignore_timestamp=False):
    monkeypatch.setattr("iotdb.utils.SessionDataSet.IoTDBRpcDataSet", FakeRpcDataSet)
    return SessionDataSet(
        "select * from root",
        ["Time", "obj"],
        ["INT64", "TEXT"],
        None,
        1,
        2,
        object(),
        3,
        [],
        ignore_timestamp,
        10,
        False,
        2,
        "UTC",
        "ms",
        None,
    )


def test_session_dataset_row_record_and_dataframe_helpers(monkeypatch):
    dataset = build_session(monkeypatch, ignore_timestamp=False)
    assert dataset.get_column_names() == ["Time", "obj"]
    assert dataset.get_column_types() == [TSDataType.INT64, TSDataType.TEXT]
    assert dataset.get_fetch_size() == 2

    assert dataset.has_next() is True
    record = dataset.next()
    assert isinstance(record, RowRecord)
    assert record.get_timestamp() == 100
    assert record.get_fields()[0].get_string_value() == "alpha"

    dataset.iotdb_rpc_data_set.buffered = True
    dataset.iotdb_rpc_data_set.more_result = True
    assert dataset.has_next_df() is True
    assert list(dataset.next_df()["obj"]) == [b"chunk-1", b"chunk-2"]
    assert dataset.todf().equals(pd.DataFrame({"obj": [b"alpha"]}))

    dataset.close_operation_handle()
    assert dataset.iotdb_rpc_data_set.closed is True


def test_session_dataset_ignore_timestamp_and_typed_point(monkeypatch):
    dataset = build_session(monkeypatch, ignore_timestamp=True)
    record = dataset.next()
    assert isinstance(record, RowRecord)
    assert record.get_timestamp() == 0
    assert record.get_fields()[0].get_string_value() == "alpha"

    assert get_typed_point(Field(TSDataType.BOOLEAN, True)) == 1
    assert get_typed_point(Field(TSDataType.TEXT, b"abc")) == "abc"
    assert get_typed_point(Field(TSDataType.FLOAT, 1.5)) == pytest.approx(1.5)
    assert get_typed_point(Field(None), none_value="missing") == "missing"

    with pytest.raises(Exception):
        get_typed_point(Field(object(), 1))

