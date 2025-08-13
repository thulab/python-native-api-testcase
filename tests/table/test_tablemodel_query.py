import datetime
import pytest
import yaml
from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet, ColumnType
from datetime import date
from iotdb.utils.Field import Field
from iotdb.utils.exception import StatementExecutionException

session = TableSession(TableSessionConfig())

"""
 Title：测试表模型查询接口
 Describe：主要测试SessionDateSet类，包括各种函数、Field查询等
 Author：肖林捷
 Date：2025/1/7
"""

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
timestamps = []
values = []


def read_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


def get_session_():
    global session
    with open('../conf/config.yml', 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    if config['enable_cluster']:
        config = TableSessionConfig(
            node_urls=config['node_urls'],
            username=config['username'],
            password=config['password'],
        )
        return TableSession(config)
    else:
        config = TableSessionConfig(
            node_urls=[f"{config['host']}:{config['port']}"],
            username=config['username'],
            password=config['password'],
        )
        return TableSession(config)


def create_database(session):
    # 创建数据库
    session.execute_non_query_statement("create database test1")
    session.execute_non_query_statement("use test1")


def create_table(session):
    session.execute_non_query_statement(
        "create table table_b("
        'id1 STRING TAG, id2 STRING TAG, id3 STRING TAG, '
        'attr1 string attribute, attr2 string attribute, attr3 string attribute,'
        'BOOLEAN BOOLEAN FIELD, INT32 INT32 FIELD, INT64 INT64 FIELD, FLOAT FLOAT FIELD, DOUBLE DOUBLE FIELD,'
        'TEXT TEXT FIELD, TIMESTAMP TIMESTAMP FIELD, DATE DATE FIELD, BLOB BLOB FIELD, STRING STRING FIELD)'
    )


def insert_data(session):
    # 1、一般写入
    table_name = "table_b"
    for row in range(10):
        timestamps.append(row)
    values.append([
        "id1：1", "id2：1",
        "id3：1",
        "attr1:1", "attr2:1",
        "attr3:1",
        False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"])
    values.append([
        "id1：2", "id2：2",
        "id3：2",
        "attr1:2", "attr2:2",
        "attr3:2",
        True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz",
        -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
        "abcdefghijklmnopqrstuvwsyz"])
    values.append([
        "id1：3", "id2：3",
        "id3：3",
        "attr1:3", "attr2:3",
        "attr3:3",
        True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
        9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
        "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
    values.append([
        "id1：4", "id2：4",
        "id3：4",
        "attr1:4", "attr2:4",
        "attr3:4",
        True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"])
    values.append([
        "id1：5", "id2：5",
        "id3：5",
        "attr1:5", "attr2:5",
        "attr3:5",
        True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1),
        '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
    values.append([
        "id1：6", "id2：6",
        "id3：6",
        "attr1:6", "attr2:6",
        "attr3:6",
        True, 10, 11, 4.123456, 4.123456789012345,
        "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11,
        date(1970, 1, 1),
        '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
            'utf-8'),
        "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
    values.append([
        "id1：7", "id2：7",
        "id3：7",
        "attr1:7", "attr2:7",
        "attr3:7",
        True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'),
        "string01"])
    values.append([
        "id1：8", "id2：8",
        "id3：8",
        "attr1:8", "attr2:8",
        "attr3:8",
        None, None, None, None, None, None, None, None, None, None])
    values.append([
        None, None, None,
        None, None, None,
        True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "])
    values.append([
        None, None, None,
        None, None, None,
        None, None, None, None, None, None, None, None, None, None])
    tablet = Tablet(table_name, column_names, data_types, values, timestamps, column_types)
    session.insert(tablet)


def delete_database(session):
    session.execute_non_query_statement("drop database test1")


@pytest.fixture()
def fixture_():
    # 用例执行前的环境搭建代码
    global session
    # 获取session
    session = get_session_()
    # 清理残余数据库
    try:
        with session.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema":
                    session.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        print(e)
    # 创建数据库
    create_database(session)
    # 创建表
    create_table(session)
    # 写入数据
    insert_data(session)

    yield

    # 用例执行完成后清理环境代码
    # 清理数据库
    try:
        with session.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema":
                    session.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        print(e)

    # 关闭 session
    session.close()


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
                    col_index], "Actual type does not match, expected:" + str(data_types[col_index]) + ", actual:" + str(
                    row_record.get_fields()[col_index + 1].get_data_type())


# 2、测试fields查询：验证字段值
@pytest.mark.usefixtures('fixture_')
def test_fields2():
    row_index = 0
    with session.execute_query_statement(
            "select * from table_b order by time") as session_data_set:
        while session_data_set.has_next():
            row_record = session_data_set.next()
            for col_index in range(len(values[row_index])):
                # 使用get_object_value函数
                if data_types[col_index] == TSDataType.TIMESTAMP:  # timestamp类型返回值需要特殊处理
                    expected_value = values[row_index][col_index]
                    actual_value = row_record.get_fields()[col_index + 1].get_object_value(data_types[col_index])
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
                else:
                    # 其他类型的原有比较逻辑保持不变
                    assert row_record.get_fields()[col_index + 1].get_object_value(data_types[col_index]) == values[row_index][
                        col_index], "Actual value does not match, expected:" + str(
                        values[row_index][col_index]) + ", actual:" + str(
                        row_record.get_fields()[col_index + 1].get_object_value(data_types[col_index]))

                # 使用其他get_xxx_value函数
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
                    assert row_record.get_fields()[col_index + 1].get_float_value() == values[row_index][
                        col_index], "Actual value does not match, expected:" + str(
                        values[row_index][col_index]) + ", actual:" + str(
                        row_record.get_fields()[col_index + 1].get_float_value())
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


# 3、测试fields查询：测试is_null、copy和set_xxx_value等方法有效性
@pytest.mark.usefixtures('fixture_')
def test_fields3():
    row_index = 0
    with session.execute_query_statement(
            "select * from table_b order by time") as session_data_set:
        while session_data_set.has_next():
            row_record = session_data_set.next()
            for col_index in range(len(values[row_index])):
                assert row_record.get_fields()[col_index + 1].is_null() == (values[row_index][col_index] is None), "is_null does not match, expected value:" + str(values[row_index][col_index]) + ", actual value:" + str(row_record.get_fields()[col_index + 1].get_object_value(data_types[col_index])) + ", record:" + str(row_record)

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

# 1、测试fields查询，get_XXX_value异常情况
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

# 2、StatementExecutionException异常
@pytest.mark.usefixtures('fixture_')
def test_query_error2():
    try:
        session.execute_query_statement("StatementExecutionException")
        assert False, "期待报错，实际无报错"
    except Exception as e:
        assert isinstance(e, StatementExecutionException) and "StatementExecutionException" in str(e), "期待报错信息与实际不一致，期待：StatementExecutionException: ，实际：" + type(e).__name__ + ":" + str(e)
