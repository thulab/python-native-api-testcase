import random

import numpy as np
import pytest
import yaml
import os
from datetime import date
from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.Session import Session
from iotdb.SessionPool import SessionPool, PoolConfig
from iotdb.table_session_pool import TableSessionPoolConfig, TableSessionPool
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet, ColumnType
from iotdb.utils.NumpyTablet import NumpyTablet
from datetime import date

"""
 Title：测试表模型python写入接口
 Describe：测试各种写入接口
 Author：肖林捷
 Date：2025/1/7
"""

# 配置文件目录
config_path = "../conf/config.yml"
database_names = ["test_insert1", "test_insert2"]

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


def create_database(session):
    # 创建数据库
    for database_name in database_names:
        session.execute_non_query_statement("create database " + database_name)
    session.execute_non_query_statement("use " + database_names[0])


def create_table(session):
    session.execute_non_query_statement(
        "create table test_insert1.table1("
        "region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, "
        "model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=3600000)"
    )
    session.execute_non_query_statement(
        "create table table2("
        "region_id STRING TAG, plant_id STRING TAG, color STRING ATTRIBUTE, temperature FLOAT FIELD,"
        " speed DOUBLE FIELD) with (TTL=6600000)"
    )
    session.execute_non_query_statement(
        "create table test_insert2.table1("
        "region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, "
        "model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=3600000)"
    )
    session.execute_non_query_statement(
        "create table test_insert2.table2("
        "region_id STRING TAG, plant_id STRING TAG, color STRING ATTRIBUTE, temperature FLOAT FIELD,"
        " speed DOUBLE FIELD) with (TTL=6600000)"
    )
    session.execute_non_query_statement(
        "create table table_a("
        '"地区" STRING TAG, "厂号" STRING TAG, "设备号" STRING TAG, '
        '"日期" string attribute, "时间" string attribute, "负责人" string attribute,'
        '"测点1" BOOLEAN FIELD, "测点2" INT32 FIELD, "测点3" INT64 FIELD, "测点4" FLOAT FIELD, "测点5" DOUBLE FIELD,'
        '"测点6" TEXT FIELD, "测点7" TIMESTAMP FIELD, "测点8" DATE FIELD, "测点9" BLOB FIELD, "测点10" STRING FIELD)'
    )
    session.execute_non_query_statement(
        "create table table_b("
        'id1 STRING TAG, id2 STRING TAG, id3 STRING TAG, '
        'attr1 string attribute, attr2 string attribute, attr3 string attribute,'
        'BOOLEAN BOOLEAN FIELD, INT32 INT32 FIELD, INT64 INT64 FIELD, FLOAT FLOAT FIELD, DOUBLE DOUBLE FIELD,'
        'TEXT TEXT FIELD, TIMESTAMP TIMESTAMP FIELD, DATE DATE FIELD, BLOB BLOB FIELD, STRING STRING FIELD)'
    )
    session.execute_non_query_statement(
        "create table table_c("
        'id1 STRING TAG, id2 STRING TAG, id3 STRING TAG, '
        'attr1 string attribute, attr2 string attribute, attr3 string attribute,'
        'BOOLEAN BOOLEAN FIELD, INT32 INT32 FIELD, INT64 INT64 FIELD, FLOAT FLOAT FIELD, DOUBLE DOUBLE FIELD,'
        'TEXT TEXT FIELD, TIMESTAMP TIMESTAMP FIELD, DATE DATE FIELD, BLOB BLOB FIELD, STRING STRING FIELD)'
    )
    session.execute_non_query_statement(
        "create table table_d("
        'id1 STRING TAG, id2 STRING TAG, id3 STRING TAG, '
        'attr1 string attribute, attr2 string attribute, attr3 string attribute,'
        'BOOLEAN BOOLEAN FIELD, INT32 INT32 FIELD, INT64 INT64 FIELD, FLOAT FLOAT FIELD, DOUBLE DOUBLE FIELD,'
        'TEXT TEXT FIELD, TIMESTAMP TIMESTAMP FIELD, DATE DATE FIELD, BLOB BLOB FIELD, STRING STRING FIELD)'
    )

def query(sql):
    actual = 0
    with session.execute_query_statement(sql) as session_data_set:
        print(session_data_set.get_column_names())
        while session_data_set.has_next():
            print(session_data_set.next())
            actual = actual + 1

        return actual


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
                if str(fields[0]) != "information_schema":
                    session.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        print(e)
    # 创建数据库
    create_database(session)
    # 创建表
    create_table(session)

    yield

    # 用例执行完成后清理环境代码
    try:
        with session.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "information_schema":
                    session.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)

    # 关闭 session
    if config['enable_cluster']:
        session.close()
    else:
        session.close()
        session_pool.close()


# 测试一般写入数据
@pytest.mark.usefixtures('fixture_')
def test_insert1():
    global session
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
        None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""])
    values.append([
        "id1：" + str(row_b), "id2：" + str(row_b), "id3：" + str(row_b),
        "attr1:" + str(row_b), "attr2:" + str(row_b), "attr3:" + str(row_b),
        True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "])
    values.append([
        "id1：" + str(row_b), "id2：" + str(row_b), "id3：" + str(row_b),
        "attr1:" + str(row_b), "attr2:" + str(row_b), "attr3:" + str(row_b),
        True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
    tablet = Tablet(table_name, column_names, data_types, values, timestamps, column_types)
    session.insert(tablet)
    # 计算实际时间序列的行数量
    actual = query("select * from table_b")
    # 判断是否符合预期
    assert expect == actual

    expect = 10
    table_name = "table_a"
    column_names = [
        "地区", "厂号", "设备号",
        "日期", "时间", "负责人",
        "测点1", "测点2", "测点3", "测点4", "测点5", "测点6", "测点7", "测点8", "测点9", "测点10"
    ]
    data_types = [
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
        TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING
    ]
    column_types = [
        ColumnType.TAG, ColumnType.TAG, ColumnType.TAG,
        ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD]
    timestamps = []
    values = []
    for row_a in range(10):
        timestamps.append(row_a)
    values.append([
        "id1：" + str(row_a), "id2：" + str(row_a), "id3：" + str(row_a),
        "attr1:" + str(row_a), "attr2:" + str(row_a), "attr3:" + str(row_a),
        False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"])
    values.append([
        "id1：" + str(row_a), "id2：" + str(row_a), "id3：" + str(row_a),
        "attr1:" + str(row_a), "attr2:" + str(row_a), "attr3:" + str(row_a),
        True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz",
        -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
        "abcdefghijklmnopqrstuvwsyz"])
    values.append([
        "id1：" + str(row_a), "id2：" + str(row_a), "id3：" + str(row_a),
        "attr1:" + str(row_a), "attr2:" + str(row_a), "attr3:" + str(row_a),
        True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
        9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
        "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
    values.append([
        "id1：" + str(row_a), "id2：" + str(row_a), "id3：" + str(row_a),
        "attr1:" + str(row_a), "attr2:" + str(row_a), "attr3:" + str(row_a),
        True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"])
    values.append([
        "id1：" + str(row_a), "id2：" + str(row_a), "id3：" + str(row_a),
        "attr1:" + str(row_a), "attr2:" + str(row_a), "attr3:" + str(row_a),
        True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1),
        '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
    values.append([
        "id1：" + str(row_a), "id2：" + str(row_a), "id3：" + str(row_a),
        "attr1:" + str(row_a), "attr2:" + str(row_a), "attr3:" + str(row_a),
        True, 10, 11, 4.123456, 4.123456789012345,
        "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11,
        date(1970, 1, 1),
        '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
            'utf-8'),
        "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
    values.append([
        "id1：" + str(row_a), "id2：" + str(row_a), "id3：" + str(row_a),
        "attr1:" + str(row_a), "attr2:" + str(row_a), "attr3:" + str(row_a),
        True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'),
        "string01"])
    values.append([
        "id1：" + str(row_a), "id2：" + str(row_a), "id3：" + str(row_a),
        "attr1:" + str(row_a), "attr2:" + str(row_a), "attr3:" + str(row_a),
        None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""])
    values.append([
        "id1：" + str(row_a), "id2：" + str(row_a), "id3：" + str(row_a),
        "attr1:" + str(row_a), "attr2:" + str(row_a), "attr3:" + str(row_a),
        True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "])
    values.append([
        "id1：" + str(row_a), "id2：" + str(row_a), "id3：" + str(row_a),
        "attr1:" + str(row_a), "attr2:" + str(row_a), "attr3:" + str(row_a),
        True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
    tablet = Tablet(table_name, column_names, data_types, values, timestamps, column_types)
    session.insert(tablet)
    # 计算实际时间序列的行数量
    actual = query("select * from table_a")
    # 判断是否符合预期
    assert expect == actual


# 测试Numpy_tablet写入数据
@pytest.mark.usefixtures('fixture_')
def test_insert2():
    global session
    # 1、不使用bitmap
    expect = 10
    table_name = "table_b"
    column_names = [
        "id1", "id2", "id3",
        "attr1", "attr2", "attr3",
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TIMESTAMP", "DATE", "BLOB", "STRING"
    ]
    data_types = [
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
        TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING
    ]
    column_types = [
        ColumnType.TAG, ColumnType.TAG, ColumnType.TAG,
        ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD]
    np_timestamps = []
    np_timestamps = np.arange(0, 10, dtype=np.dtype(">i8"))
    np_values = [
        np.array(["id1:{}".format(i) for i in range(0, 10)]), np.array(["id2:{}".format(i) for i in range(0, 10)]),
        np.array(["id3:{}".format(i) for i in range(0, 10)]),
        np.array(["attr1:{}".format(i) for i in range(0, 10)]), np.array(["attr2:{}".format(i) for i in range(0, 10)]),
        np.array(["attr3:{}".format(i) for i in range(0, 10)]),
        np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10],
                 TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1],
                 TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345,
                  12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题",
                  "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                  "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                  "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10],
                 TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1),
                  date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)],
                 TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
                  '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'),
                  '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'),
                  '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                      'utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'),
                  '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题",
                  "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                  "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                  "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_tablet = NumpyTablet(
        table_name,
        column_names,
        data_types,
        np_values,
        np_timestamps,
        column_types=column_types
    )
    session.insert(np_tablet)
    # 计算实际时间序列的行数量
    actual = query("select * from table_b")
    # 判断是否符合预期
    assert expect == actual

    expect = 10
    # 2、使用bitmap
    table_name = "table_d"
    column_names = [
        "id1", "id2", "id3",
        "attr1", "attr2", "attr3",
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TIMESTAMP", "DATE", "BLOB", "STRING"
    ]
    data_types = [
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
        TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING
    ]
    column_types = [
        ColumnType.TAG, ColumnType.TAG, ColumnType.TAG,
        ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD]
    np_timestamps = np.arange(0, 10, dtype=np.dtype(">i8"))
    np_values = [
        np.array(["id1:{}".format(i) for i in range(0, 10)]), np.array(["id2:{}".format(i) for i in range(0, 10)]),
        np.array(["id3:{}".format(i) for i in range(0, 10)]),
        np.array(["attr1:{}".format(i) for i in range(0, 10)]), np.array(["attr2:{}".format(i) for i in range(0, 10)]),
        np.array(["attr3:{}".format(i) for i in range(0, 10)]),
        np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
        np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10],
                 TSDataType.INT64.np_dtype()),
        np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1],
                 TSDataType.FLOAT.np_dtype()),
        np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345,
                  12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题",
                  "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                  "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                  "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
        np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10],
                 TSDataType.TIMESTAMP.np_dtype()),
        np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1),
                  date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)],
                 TSDataType.DATE.np_dtype()),
        np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
                  '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'),
                  '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'),
                  '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                      'utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'),
                  '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
        np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题",
                  "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                  "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                  "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
    ]
    np_bitmaps_ = []
    for i in range(len(column_types)):
        np_bitmaps_.append(BitMap(len(np_timestamps)))
    np_bitmaps_[5].mark(9)
    np_bitmaps_[6].mark(8)
    np_bitmaps_[7].mark(9)
    np_bitmaps_[8].mark(8)
    np_bitmaps_[9].mark(9)
    np_bitmaps_[10].mark(8)
    np_bitmaps_[11].mark(9)
    np_bitmaps_[12].mark(8)
    np_bitmaps_[13].mark(9)
    np_bitmaps_[14].mark(8)
    np_tablet = NumpyTablet(
        table_name,
        column_names,
        data_types,
        np_values,
        np_timestamps,
        bitmaps=np_bitmaps_,
        column_types=column_types
    )
    session.insert(np_tablet)
    # 计算实际时间序列的行数量
    actual = query("select * from table_d")
    # 判断是否符合预期
    assert expect == actual


# 测试直接写入数据
@pytest.mark.usefixtures('fixture_')
def test_insert3():
    global session
    for i in range(1, 10):
        table_name = "t" + str(i)
        column_names = [
            "id1", "id2", "id3",
            "attr1", "attr2", "attr3",
            "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TIMESTAMP", "DATE", "BLOB", "STRING"
        ]
        data_types = [
            TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
            TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
            TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
            TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING
        ]
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
        values.append([
            "id1：" + str(row), "id2：" + str(row), "id3：" + str(row),
            "attr1:" + str(row), "attr2:" + str(row), "attr3:" + str(row),
            True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz",
            -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
            "abcdefghijklmnopqrstuvwsyz"])
        values.append([
            "id1：" + str(row), "id2：" + str(row), "id3：" + str(row),
            "attr1:" + str(row), "attr2:" + str(row), "attr3:" + str(row),
            True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
            9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
            "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
        values.append([
            "id1：" + str(row), "id2：" + str(row), "id3：" + str(row),
            "attr1:" + str(row), "attr2:" + str(row), "attr3:" + str(row),
            True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"])
        values.append([
            "id1：" + str(row), "id2：" + str(row), "id3：" + str(row),
            "attr1:" + str(row), "attr2:" + str(row), "attr3:" + str(row),
            True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1),
            '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
        values.append([
            "id1：" + str(row), "id2：" + str(row), "id3：" + str(row),
            "attr1:" + str(row), "attr2:" + str(row), "attr3:" + str(row),
            True, 10, 11, 4.123456, 4.123456789012345,
            "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11,
            date(1970, 1, 1),
            '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                'utf-8'),
            "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
        values.append([
            "id1：" + str(row), "id2：" + str(row), "id3：" + str(row),
            "attr1:" + str(row), "attr2:" + str(row), "attr3:" + str(row),
            True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
            'Hello, World!'.encode('utf-8'), "string01"])
        values.append([
            "id1：" + str(row), "id2：" + str(row), "id3：" + str(row),
            "attr1:" + str(row), "attr2:" + str(row), "attr3:" + str(row),
            None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""])
        values.append([
            "id1：" + str(row), "id2：" + str(row), "id3：" + str(row),
            "attr1:" + str(row), "attr2:" + str(row), "attr3:" + str(row),
            True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "])
        values.append([
            "id1：" + str(row), "id2：" + str(row), "id3：" + str(row),
            "attr1:" + str(row), "attr2:" + str(row), "attr3:" + str(row),
            True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
        tablet = Tablet(table_name, column_names, data_types, values, timestamps, column_types)
        session.insert(tablet)
    # 测试一般写入数据


# 乱序写入：测试时间戳自动排序功能
@pytest.mark.usefixtures('fixture_')
def test_insert4():
    global session
    table_name = "table_b"
    column_names = [
        "id1", "id2", "id3",
        "attr1", "attr2", "attr3",
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TIMESTAMP", "DATE", "BLOB", "STRING"
    ]
    data_types = [
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.STRING, TSDataType.STRING, TSDataType.STRING,
        TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
        TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING
    ]
    column_types = [
        ColumnType.TAG, ColumnType.TAG, ColumnType.TAG,
        ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE, ColumnType.ATTRIBUTE,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD,
        ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD
    ]

    # 乱序的时间戳和对应的值
    timestamps = []
    values = []
    for num in range(100):
        timestamps.append(random.randint(-10000, 10000))
        values.append(["id1：" + str(num), "id2：" + str(num), "id3：" + str(num),
            "attr1:" + str(num), "attr2:" + str(num), "attr3:" + str(num),
            False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"])


    # 创建 Tablet 实例
    tablet = Tablet(table_name, column_names, data_types, values, timestamps, column_types)
    session.insert(tablet)


# 错误情况：时间戳长度和值长度不一致
@pytest.mark.usefixtures('fixture_')
def test_insert_error1():
    global session
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
    timestamps = []
    values = []
    for row_a in range(10):
        timestamps.append(row_a)
    for row_b in range(1):
        values.append([
            "id1：" + str(row_b), "id2：" + str(row_b), "id3：" + str(row_b),
            "attr1:" + str(row_b), "attr2:" + str(row_b), "attr3:" + str(row_b),
            False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"])
    try:
        Tablet(table_name, column_names, data_types, values, timestamps, column_types)
        assert False, "期待报错，实际无报错"
    except Exception as e:
        assert isinstance(e, RuntimeError) and str(e) == "Input error! len(timestamps) does not equal to len(values)!", "期待报错信息与实际不一致，期待：RuntimeError：Input error! len(timestamps) does not equal to len(values)!，实际：" + type(e).__name__ + ":" + str(e)




