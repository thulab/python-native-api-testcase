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

"""
 Title：测试表模型python查询接口—正常情况
 Describe：基于表模型版本，测试各种查询接口
 Author：肖林捷
 Date：2025/1/7
"""

# session = TableSession()
session = None


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
    expect = 10
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
    timestamps = []
    values = []
    for row_b in range(10):
        timestamps.append(row_b)
    values.append([
        "id1：" + str(row_b), "id2：" + str(row_b), "id3：" + str(row_b),
        "attr1:" + str(row_b), "attr2:" + str(row_b), "attr3:" + str(row_b),
        False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"])
    values.append([
        "id1：" + str(row_b), "id2：" + str(row_b), "id3：" + str(row_b),
        "attr1:" + str(row_b), "attr2:" + str(row_b), "attr3:" + str(row_b),
        True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz",
        -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
        "abcdefghijklmnopqrstuvwsyz"])
    values.append([
        "id1：" + str(row_b), "id2：" + str(row_b), "id3：" + str(row_b),
        "attr1:" + str(row_b), "attr2:" + str(row_b), "attr3:" + str(row_b),
        True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
        9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
        "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
    values.append([
        "id1：" + str(row_b), "id2：" + str(row_b), "id3：" + str(row_b),
        "attr1:" + str(row_b), "attr2:" + str(row_b), "attr3:" + str(row_b),
        True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"])
    values.append([
        "id1：" + str(row_b), "id2：" + str(row_b), "id3：" + str(row_b),
        "attr1:" + str(row_b), "attr2:" + str(row_b), "attr3:" + str(row_b),
        True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1),
        '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
    values.append([
        "id1：" + str(row_b), "id2：" + str(row_b), "id3：" + str(row_b),
        "attr1:" + str(row_b), "attr2:" + str(row_b), "attr3:" + str(row_b),
        True, 10, 11, 4.123456, 4.123456789012345,
        "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11,
        date(1970, 1, 1),
        '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
            'utf-8'),
        "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
    values.append([
        "id1：" + str(row_b), "id2：" + str(row_b), "id3：" + str(row_b),
        "attr1:" + str(row_b), "attr2:" + str(row_b), "attr3:" + str(row_b),
        True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'),
        "string01"])
    values.append([
        "id1：" + str(row_b), "id2：" + str(row_b), "id3：" + str(row_b),
        "attr1:" + str(row_b), "attr2:" + str(row_b), "attr3:" + str(row_b),
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


# 测试查询
@pytest.mark.usefixtures('fixture_')
def test_query():
    expect = 10
    actual = 0
    with session.execute_query_statement("select id1,id2,id3,attr1,attr2,attr3,BOOLEAN,INT32,INT64,FLOAT,DOUBLE,TEXT,DATE,TIMESTAMP,BLOB,STRING from table_b") as session_data_set:
        print(session_data_set.get_column_names())
        print(session_data_set.get_column_types())
        session_data_set.get_column_types()
        while session_data_set.has_next():
            row_record = session_data_set.next()
            row_record.get_fields()[0].copy(row_record.get_fields()[1])
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
            # print(row_record.get_fields()[6].get_data_type())
            row_record.get_fields()[2].get_data_type()
            row_record.get_fields()[3].is_null()
            row_record.get_fields()[4].get_object_value(TSDataType.STRING)
            row_record.get_fields()[5].get_field(row_record.get_fields()[5].get_object_value(TSDataType.STRING), TSDataType.STRING)
            row_record.get_fields()[6].set_bool_value(False)
            row_record.get_fields()[6].get_bool_value()
            row_record.get_fields()[6].get_object_value(TSDataType.BOOLEAN)
            row_record.get_fields()[7].set_int_value(10)
            row_record.get_fields()[7].get_int_value()
            row_record.get_fields()[7].get_object_value(TSDataType.INT32)
            row_record.get_fields()[8].set_long_value(10)
            row_record.get_fields()[8].get_long_value()
            row_record.get_fields()[8].get_object_value(TSDataType.INT64)
            row_record.get_fields()[9].set_float_value(1.1)
            row_record.get_fields()[9].get_float_value()
            row_record.get_fields()[9].get_object_value(TSDataType.FLOAT)
            row_record.get_fields()[10].set_double_value(1.1)
            row_record.get_fields()[10].get_double_value()
            row_record.get_fields()[10].get_string_value()
            row_record.get_fields()[10].get_object_value(TSDataType.DOUBLE)
            row_record.get_fields()[11].get_string_value()
            row_record.get_fields()[11].get_object_value(TSDataType.TEXT)
            row_record.get_fields()[12].get_date_value()
            row_record.get_fields()[12].get_string_value()
            row_record.get_fields()[12].get_object_value(TSDataType.DATE)
            row_record.get_fields()[13].get_timestamp_value()
            row_record.get_fields()[13].get_object_value(TSDataType.TIMESTAMP)
            row_record.get_fields()[14].set_binary_value('1'.encode('utf-8'))
            row_record.get_fields()[14].get_binary_value()
            row_record.get_fields()[14].get_string_value()
            row_record.get_fields()[14].get_object_value(TSDataType.BLOB)
            row_record.get_fields()[15].get_string_value()
            row_record.get_fields()[15].get_object_value(TSDataType.STRING)
            actual = actual + 1

        assert expect == actual, "Actual number of rows does not match, expected:" + str(expect) + ", actual:" + str(
            actual)
