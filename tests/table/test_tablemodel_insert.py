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

# ===== merged from test_object_column.py =====
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
OBJECT 类型完整测试 - Python SDK

功能：
1. 每个测试执行完后自动清理环境
2. 支持单独运行指定测试用例查看详细错误
3. 详细的错误输出和调试信息
4. 默认连接远程服务器 (127.0.0.1:6667)

运行方式：
# 运行全部测试（使用默认服务器）
python test_object_column.py

# 运行指定测试类
python test_object_column.py --class TestTabletWrite

# 运行指定测试方法
python test_object_column.py --class TestTabletWrite --method test_01_basic_write

# 使用其他服务器
python test_object_column.py --host localhost --port 6667 --user root --password root

# pytest 方式运行
pytest test_object_column.py -v -s
"""

import pytest
from iotdb.utils.object_column import decode_object_cell, encode_object_cell


# ============================================================================
# 全局配置
# ============================================================================

_session = None
_config = None
_tables_created = []  # 记录创建的表，用于清理


def object_init_session(host, port, username, password):
    """初始化连接"""
    global _session, _config, _tables_created
    _config = {
        "host": host,
        "port": port,
        "username": username,
        "password": password
    }

    config = TableSessionConfig(
        node_urls=[f"{host}:{port}"],
        username=username,
        password=password
    )
    _session = TableSession(config)
    _tables_created = []

    # 创建测试数据库
    try:
        _session.execute_non_query_statement("USE information_schema")
    except:
        pass
    try:
        _session.execute_non_query_statement("DROP DATABASE test_object_py")
    except:
        pass
    try:
        _session.execute_non_query_statement("CREATE DATABASE test_object_py")
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise
    _session.execute_non_query_statement("USE test_object_py")

    return _session


def object_get_session():
    return _session


def object_close_session():
    """关闭连接并清理所有环境"""
    global _session, _tables_created

    if _session is not None:
        # 清理创建的表
        for table in _tables_created:
            try:
                _session.execute_non_query_statement(f"DROP TABLE {table}")
            except Exception as e:
                pass

        # 切回系统库后再删测试库，避免当前库被占用导致 DROP 失败。
        try:
            _session.execute_non_query_statement("USE information_schema")
        except:
            pass

        # 删除整个数据库
        try:
            _session.execute_non_query_statement("DROP DATABASE test_object_py")
        except:
            pass

        _session.close()
        _session = None
        _tables_created = []


def object_cleanup_tables():
    """清理当前测试创建的表"""
    global _session, _tables_created

    for table in _tables_created:
        try:
            _session.execute_non_query_statement(f"DROP TABLE {table}")
        except:
            pass
    _tables_created = []


def object_create_table(sql):
    """创建表并记录"""
    global _session, _tables_created
    _session.execute_non_query_statement(sql)
    # 提取表名
    table_name = sql.split("CREATE TABLE")[1].split("(")[0].strip()
    _tables_created.append(table_name)


def read_object_content(
    table_name, where_clause="", order_by_time=True, column_name="obj_col"
):
    """使用 READ_OBJECT 函数读取内容。

    仅适用于预期所有结果都能成功读取真实二进制的场景。
    """
    global _session

    sql = f"SELECT READ_OBJECT({column_name}) FROM {table_name}"
    if where_clause:
        sql += f" WHERE {where_clause}"
    if order_by_time:
        sql += " ORDER BY time"

    result = []
    try:
        with _session.execute_query_statement(sql) as rs:
            while rs.has_next():
                row = rs.next()
                fields = row.get_fields()
                blob = fields[0].get_binary_value()
                result.append(blob)
    except Exception as e:
        print(f"    READ_OBJECT error: {e}")
        raise

    print(
        f"    READ_OBJECT result [{table_name}.{column_name}]"
        f"{' WHERE ' + where_clause if where_clause else ''}: {result}"
    )
    return result


def read_object_display(
    table_name, where_clause="", order_by_time=True, column_name="obj_col"
):
    """读取 OBJECT 列的默认展示形式。

    文档语义：
    - NULL 展示为 null
    - 非空 OBJECT 展示为 (Object) X B/KB/MB
    """
    global _session

    sql = f"SELECT {column_name} FROM {table_name}"
    if where_clause:
        sql += f" WHERE {where_clause}"
    if order_by_time:
        sql += " ORDER BY time"

    result = []
    with _session.execute_query_statement(sql) as rs:
        while rs.has_next():
            row = rs.next()
            result.append(row.get_fields()[0].get_string_value())
    print(
        f"    DISPLAY result [{table_name}.{column_name}]"
        f"{' WHERE ' + where_clause if where_clause else ''}: {result}"
    )
    return result


def assert_read_object_fails(
    table_name, where_clause="", expected_message_parts=None, column_name="obj_col"
):
    """断言 READ_OBJECT 查询失败，并校验关键报错片段。"""
    global _session

    sql = f"SELECT READ_OBJECT({column_name}) FROM {table_name}"
    if where_clause:
        sql += f" WHERE {where_clause}"
    sql += " ORDER BY time"

    with pytest.raises(Exception) as excinfo:
        with _session.execute_query_statement(sql) as rs:
            while rs.has_next():
                rs.next()

    message = str(excinfo.value)
    print(f"    Expected READ_OBJECT failure: {message}")
    if expected_message_parts:
        for part in expected_message_parts:
            assert part in message
    return message


def assert_object_display(display_value, expected_size=None, expect_null=False):
    """断言 OBJECT 默认展示语义。"""
    if expect_null:
        assert display_value is None or str(display_value).lower() in ("null", "none", "")
        return

    assert display_value is not None
    text = str(display_value)
    assert text.lower() not in ("null", "none", "")
    if expected_size is not None:
        if text.startswith("(Object)"):
            assert f"{expected_size} B" in text


def fetch_query_rows(sql, value_getter="string"):
    """执行查询并返回每行字段值，便于函数测试复用。"""
    global _session

    rows = []
    with _session.execute_query_statement(sql) as rs:
        while rs.has_next():
            row = rs.next()
            fields = row.get_fields()
            values = []
            for field in fields:
                if value_getter == "binary":
                    values.append(field.get_binary_value())
                elif value_getter == "int":
                    values.append(field.get_long_value())
                elif value_getter == "object":
                    values.append(field.get_object_value(field.get_data_type()))
                else:
                    values.append(field.get_string_value())
            rows.append(values)
    print(f"    QUERY rows ({value_getter}): {sql} -> {rows}")
    return rows


def assert_query_fails(sql, expected_message_parts=None):
    """断言 SQL 查询失败，并校验关键报错片段。"""
    global _session

    with pytest.raises(Exception) as excinfo:
        with _session.execute_query_statement(sql) as rs:
            while rs.has_next():
                rs.next()

    message = str(excinfo.value)
    print(f"    Expected query failure for `{sql}`: {message}")
    if expected_message_parts:
        for part in expected_message_parts:
            assert part in message
    return message


# ============================================================================
# 编解码测试 - 不需要 IoTDB
# ============================================================================

class TestObjectColumnEncodeDecode:
    """编解码函数测试"""

    def test_encode_decode_roundtrip(self):
        payload = b"\xca\xfe\xba\xbe"
        cell = encode_object_cell(True, 0, payload)
        is_eof, offset, body = decode_object_cell(cell)
        assert is_eof is True
        assert offset == 0
        assert body == payload

    def test_encode_decode_segment(self):
        cell = encode_object_cell(False, 512, b"\x01\x02")
        is_eof, offset, body = decode_object_cell(cell)
        assert is_eof is False
        assert offset == 512
        assert body == b"\x01\x02"

    def test_empty_payload_encode(self):
        cell = encode_object_cell(True, 0, b"")
        is_eof, offset, body = decode_object_cell(cell)
        assert is_eof is True
        assert offset == 0
        assert body == b""

    def test_large_payload_encode(self):
        large_payload = b"x" * 10240
        cell = encode_object_cell(True, 0, large_payload)
        is_eof, offset, body = decode_object_cell(cell)
        assert body == large_payload

    def test_mb_payload_encode(self):
        mb_payload = b"x" * (1024 * 1024)
        cell = encode_object_cell(True, 0, mb_payload)
        is_eof, offset, body = decode_object_cell(cell)
        assert body == mb_payload

    def test_decode_cell_too_short(self):
        with pytest.raises(ValueError):
            decode_object_cell(b"\x00\x00\x00\x00\x00\x00\x00\x00")

    def test_encode_content_not_bytes(self):
        with pytest.raises(TypeError):
            encode_object_cell(True, 0, "string")

    def test_encode_accepts_bytearray(self):
        cell = encode_object_cell(True, 0, bytearray(b"test"))
        is_eof, _, body = decode_object_cell(cell)
        assert body == b"test"


# ============================================================================
# Tablet 写入测试
# ============================================================================

class ObjectSessionTestBase:
    @classmethod
    def setup_class(cls):
        object_init_session("127.0.0.1", 6667, "root", "TimechoDB@2021")

    @classmethod
    def teardown_class(cls):
        object_close_session()


class TestTabletWrite(ObjectSessionTestBase):
    """Tablet 写入 OBJECT 测试"""

    def setup_method(self, method):
        """每个测试前初始化"""
        object_cleanup_tables()

    def teardown_method(self, method):
        """每个测试后清理"""
        object_cleanup_tables()

    def test_01_basic_write(self):
        """基本写入验证"""
        session = object_get_session()

        object_create_table("CREATE TABLE t_basic(device STRING TAG, obj_col OBJECT FIELD)")

        payload = b"\xca\xfe\xba\xbe"

        tablet = Tablet("t_basic",
            ["device", "obj_col"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["d1", None]], [1],
            [ColumnType.TAG, ColumnType.FIELD])

        tablet.add_value_object(0, 1, True, 0, payload)
        session.insert(tablet)

        result = read_object_content("t_basic")
        print(f"    Expected: {payload.hex()}, Got: {result[0].hex() if result[0] else 'None'}")
        assert len(result) == 1
        assert result[0] == payload

    def test_02_multiple_rows(self):
        """多行写入"""
        session = object_get_session()

        object_create_table("CREATE TABLE t_multi(device STRING TAG, obj_col OBJECT FIELD)")

        payloads = [b"\x01\x02", b"\x03\x04", b"\x05\x06"]

        tablet = Tablet("t_multi",
            ["device", "obj_col"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["d1", None], ["d2", None], ["d3", None]],
            [1, 2, 3],
            [ColumnType.TAG, ColumnType.FIELD])

        # 分别写入每行，验证写入过程
        print("    Writing row 0...")
        tablet.add_value_object(0, 1, True, 0, payloads[0])
        print("    Writing row 1...")
        tablet.add_value_object(1, 1, True, 0, payloads[1])
        print("    Writing row 2...")
        tablet.add_value_object(2, 1, True, 0, payloads[2])
        session.insert(tablet)

        # 先用基本查询查看数据
        print("    === Basic SELECT * query ===")
        try:
            with session.execute_query_statement("SELECT * FROM t_multi ORDER BY time") as result:
                count = 0
                while result.has_next():
                    row = result.next()
                    fields = row.get_fields()
                    obj_field = fields[1]  # obj_col is field index 1
                    print(f"    Row {count}: device={fields[0].get_string_value()}, obj type={obj_field.get_data_type()}, obj value={obj_field.get_string_value()}")
                    count += 1
        except Exception as e:
            print(f"    Basic query error: {e}")

        # 再用 READ_OBJECT 查询
        print("    === READ_OBJECT query ===")
        result = read_object_content("t_multi")
        print(f"    Expected 3 rows, Got: {len(result)} rows")
        for i, r in enumerate(result):
            print(f"    Row {i}: Expected {payloads[i].hex()}, Got {r.hex() if r else 'None'}")

        assert len(result) == 3
        for i, p in enumerate(payloads):
            assert result[i] == p

    def test_03_multiple_columns(self):
        """多 OBJECT 列"""
        session = object_get_session()

        object_create_table("CREATE TABLE t_cols(device STRING TAG, o1 OBJECT FIELD, o2 OBJECT FIELD, o3 OBJECT FIELD)")

        payloads = [b"obj1", b"obj2", b"obj3"]

        tablet = Tablet("t_cols",
            ["device", "o1", "o2", "o3"],
            [TSDataType.STRING, TSDataType.OBJECT, TSDataType.OBJECT, TSDataType.OBJECT],
            [["d1", None, None, None]], [1],
            [ColumnType.TAG, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD])

        tablet.add_value_object(0, 1, True, 0, payloads[0])
        tablet.add_value_object(0, 2, True, 0, payloads[1])
        tablet.add_value_object(0, 3, True, 0, payloads[2])
        session.insert(tablet)

        # 验证每列
        with session.execute_query_statement(
            "SELECT READ_OBJECT(o1), READ_OBJECT(o2), READ_OBJECT(o3) FROM t_cols"
        ) as result:
            row = result.next()
            fields = row.get_fields()
            for i in range(3):
                got = fields[i].get_binary_value()
                print(f"    Column {i}: Expected {payloads[i]}, Got {got}")
                assert got == payloads[i]

    def test_04_mixed_columns(self):
        """混合类型列"""
        session = object_get_session()

        object_create_table("CREATE TABLE t_mixed(device STRING TAG, i INT32 FIELD, f FLOAT FIELD, s STRING FIELD, obj OBJECT FIELD)")

        payloads = [b"\xaa\xbb", b"\xcc\xdd"]

        # 测试1: 单行写入
        print("    === Test 1: Single row with mixed columns ===")
        tablet1 = Tablet("t_mixed",
            ["device", "i", "f", "s", "obj"],
            [TSDataType.STRING, TSDataType.INT32, TSDataType.FLOAT, TSDataType.STRING, TSDataType.OBJECT],
            [["d1", 100, 1.5, "hello", None]], [1],
            [ColumnType.TAG, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD])

        tablet1.add_value_object(0, 4, True, 0, payloads[0])
        session.insert(tablet1)

        with session.execute_query_statement("SELECT i, f, s, obj FROM t_mixed") as result:
            row = result.next()
            fields = row.get_fields()
            assert fields[0].get_int_value() == 100
            assert abs(fields[1].get_float_value() - 1.5) < 1e-6
            assert fields[2].get_string_value() == "hello"
            assert_object_display(fields[3].get_string_value(), expected_size=2)

        single_row = read_object_content("t_mixed", column_name="obj")
        assert single_row == [payloads[0]]

        # 清理表重新测试多行
        object_cleanup_tables()
        object_create_table("CREATE TABLE t_mixed(device STRING TAG, i INT32 FIELD, f FLOAT FIELD, s STRING FIELD, obj OBJECT FIELD)")

        print("    === Test 2: Multiple rows ===")
        tablet2 = Tablet("t_mixed",
            ["device", "i", "f", "s", "obj"],
            [TSDataType.STRING, TSDataType.INT32, TSDataType.FLOAT, TSDataType.STRING, TSDataType.OBJECT],
            [["d1", 100, 1.5, "hello", None], ["d2", 200, 2.5, "world", None]],
            [1, 2],
            [ColumnType.TAG, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD])

        tablet2.add_value_object(0, 4, True, 0, payloads[0])
        tablet2.add_value_object(1, 4, True, 0, payloads[1])
        session.insert(tablet2)

        with session.execute_query_statement("SELECT i, f, s, obj FROM t_mixed ORDER BY time") as result:
            rows = []
            while result.has_next():
                row = result.next()
                fields = row.get_fields()
                rows.append(
                    (
                        fields[0].get_int_value(),
                        fields[1].get_float_value(),
                        fields[2].get_string_value(),
                        fields[3].get_string_value(),
                    )
                )

        assert len(rows) == 2
        assert rows[0][0] == 100
        assert rows[1][0] == 200
        assert abs(rows[0][1] - 1.5) < 1e-6
        assert abs(rows[1][1] - 2.5) < 1e-6
        assert rows[0][2] == "hello"
        assert rows[1][2] == "world"
        assert_object_display(rows[0][3], expected_size=2)
        assert_object_display(rows[1][3], expected_size=2)

        multi_row = read_object_content("t_mixed", column_name="obj")
        assert multi_row == payloads

    def test_05_add_by_name(self):
        """通过列名写入"""
        session = object_get_session()

        object_create_table("CREATE TABLE t_name(device STRING TAG, obj_col OBJECT FIELD)")

        payload = b"test_data"

        tablet = Tablet("t_name",
            ["device", "obj_col"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["d1", None]], [1],
            [ColumnType.TAG, ColumnType.FIELD])

        tablet.add_value_object_by_name("obj_col", 0, True, 0, payload)
        session.insert(tablet)

        result = read_object_content("t_name")
        assert result[0] == payload


# ============================================================================
# 空值测试
# ============================================================================

class TestNullValues(ObjectSessionTestBase):
    """空值测试"""

    def setup_method(self, method):
        object_cleanup_tables()

    def teardown_method(self, method):
        object_cleanup_tables()

    def test_01_empty_payload(self):
        """空 payload b''"""
        session = object_get_session()

        object_create_table("CREATE TABLE n_empty(device STRING TAG, obj_col OBJECT FIELD)")

        payload = b''

        tablet = Tablet("n_empty",
            ["device", "obj_col"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["d1", None]], [1],
            [ColumnType.TAG, ColumnType.FIELD])

        tablet.add_value_object(0, 1, True, 0, payload)
        session.insert(tablet)

        display = read_object_display("n_empty")
        print(f"    Empty payload display: {display}")
        assert len(display) == 1
        assert_object_display(display[0], expected_size=0)
        assert_read_object_fails(
            "n_empty",
            expected_message_parts=["offset", "object size 0"],
        )

    def test_02_sql_null(self):
        """SQL NULL"""
        session = object_get_session()

        object_create_table("CREATE TABLE n_sql(device STRING TAG, obj_col OBJECT FIELD)")

        session.execute_non_query_statement(
            "INSERT INTO n_sql(time, device, obj_col) VALUES(1, 'd1', null)"
        )

        display = read_object_display("n_sql")
        print(f"    SQL NULL display: {display}")
        assert len(display) == 1
        assert_object_display(display[0], expect_null=True)

        with session.execute_query_statement(
            "SELECT COUNT(*) FROM n_sql WHERE obj_col IS NULL"
        ) as result:
            row = result.next()
            assert row.get_fields()[0].get_long_value() == 1

        with session.execute_query_statement(
            "SELECT COUNT(*) FROM n_sql WHERE obj_col IS NOT NULL"
        ) as result:
            row = result.next()
            assert row.get_fields()[0].get_long_value() == 0

    def test_03_tablet_partial_null(self):
        """Tablet 部分行 NULL"""
        session = object_get_session()

        object_create_table("CREATE TABLE n_partial(device STRING TAG, obj_col OBJECT FIELD)")

        payloads = {0: b"row1", 2: b"row3"}  # 只填第1、3行

        tablet = Tablet("n_partial",
            ["device", "obj_col"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["d1", None], ["d2", None], ["d3", None]],
            [1, 2, 3],
            [ColumnType.TAG, ColumnType.FIELD])

        for row_idx, p in payloads.items():
            tablet.add_value_object(row_idx, 1, True, 0, p)
        session.insert(tablet)

        display = read_object_display("n_partial")
        print(f"    Partial NULL display: {display}")
        assert len(display) == 3
        assert_object_display(display[0], expected_size=4)
        assert_object_display(display[1], expect_null=True)
        assert_object_display(display[2], expected_size=4)

        assert read_object_content("n_partial", "time = 1") == [b"row1"]
        assert read_object_content("n_partial", "time = 3") == [b"row3"]

        with session.execute_query_statement(
            "SELECT COUNT(*) FROM n_partial WHERE obj_col IS NULL"
        ) as result:
            row = result.next()
            assert row.get_fields()[0].get_long_value() == 1

    def test_04_alternating_null(self):
        """OBJECT 与 NULL 交替"""
        session = object_get_session()

        object_create_table("CREATE TABLE n_alt(device STRING TAG, obj_col OBJECT FIELD)")

        for i in range(1, 11):
            if i % 2 == 1:
                session.execute_non_query_statement(
                    f"INSERT INTO n_alt(time, device, obj_col) VALUES({i}, 'tag1', to_object(true, 0, X'62'))"
                )
            else:
                session.execute_non_query_statement(
                    f"INSERT INTO n_alt(time, device, obj_col) VALUES({i}, 'tag1', null)"
                )

        display = read_object_display("n_alt")
        print(f"    Alternating display: {display}")
        assert len(display) == 10
        for i, value in enumerate(display):
            if i % 2 == 0:
                assert_object_display(value, expected_size=1)
            else:
                assert_object_display(value, expect_null=True)

        non_null = read_object_content("n_alt", "obj_col IS NOT NULL")
        assert non_null == [b"\x62"] * 5

        with session.execute_query_statement(
            "SELECT COUNT(*) FROM n_alt WHERE obj_col IS NULL"
        ) as result:
            row = result.next()
            assert row.get_fields()[0].get_long_value() == 5


# ============================================================================
# SQL 写入测试
# ============================================================================

class TestSQLWrite(ObjectSessionTestBase):
    """SQL to_object 测试"""

    def setup_method(self, method):
        object_cleanup_tables()

    def teardown_method(self, method):
        object_cleanup_tables()

    def test_01_sql_to_object(self):
        """SQL to_object 函数"""
        session = object_get_session()

        object_create_table("CREATE TABLE sql_to(device STRING TAG, obj_col OBJECT FIELD)")

        payload = b"\xca\xfe\xba\xbe"

        session.execute_non_query_statement(
            "INSERT INTO sql_to(time, device, obj_col) VALUES(1, 'd1', to_object(true, 0, X'cafebabe'))"
        )

        result = read_object_content("sql_to")
        print(f"    SQL to_object: Expected {payload.hex()}, Got {result[0].hex()}")
        assert result[0] == payload

    def test_02_sql_vs_tablet(self):
        """SQL 与 Tablet 结果对比"""
        session = object_get_session()

        object_create_table("CREATE TABLE sql_vs(device STRING TAG, obj_col OBJECT FIELD)")

        payload = b"\xde\xad\xbe\xef"

        # SQL
        session.execute_non_query_statement(
            "INSERT INTO sql_vs(time, device, obj_col) VALUES(1, 'sql', to_object(true, 0, X'deadbeef'))"
        )

        # Tablet
        tablet = Tablet("sql_vs",
            ["device", "obj_col"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["tablet", None]], [2],
            [ColumnType.TAG, ColumnType.FIELD])
        tablet.add_value_object(0, 1, True, 0, payload)
        session.insert(tablet)

        result = read_object_content("sql_vs")
        print(f"    SQL vs Tablet: {len(result)} rows")
        for i, r in enumerate(result):
            print(f"    Row {i}: {r.hex()}")
        assert result[0] == payload
        assert result[1] == payload

    def test_03_read_object_fails_with_zero_byte_row(self):
        """结果集中含 0B OBJECT 时，整批 READ_OBJECT 查询失败"""
        session = object_get_session()

        object_create_table("CREATE TABLE sql_zero_mix(device STRING TAG, obj_col OBJECT FIELD)")

        session.execute_non_query_statement(
            "INSERT INTO sql_zero_mix(time, device, obj_col) VALUES(1, 'd1', to_object(true, 0, X'1234'))"
        )
        session.execute_non_query_statement(
            "INSERT INTO sql_zero_mix(time, device, obj_col) VALUES(2, 'd2', to_object(true, 0, X''))"
        )

        display = read_object_display("sql_zero_mix")
        assert len(display) == 2
        assert_object_display(display[0], expected_size=2)
        assert_object_display(display[1], expected_size=0)

        assert_read_object_fails(
            "sql_zero_mix",
            expected_message_parts=["offset", "object size 0"],
        )

    def test_04_read_object_succeeds_after_filtering_to_normal_row(self):
        """过滤到正常 OBJECT 行后，READ_OBJECT 可以成功读取"""
        session = object_get_session()

        object_create_table("CREATE TABLE sql_zero_filter(device STRING TAG, obj_col OBJECT FIELD)")

        session.execute_non_query_statement(
            "INSERT INTO sql_zero_filter(time, device, obj_col) VALUES(1, 'd1', to_object(true, 0, X'1234'))"
        )
        session.execute_non_query_statement(
            "INSERT INTO sql_zero_filter(time, device, obj_col) VALUES(2, 'd2', to_object(true, 0, X''))"
        )

        assert read_object_content("sql_zero_filter", "time = 1") == [b"\x12\x34"]

    def test_05_read_object_fails_after_filtering_to_zero_byte_row(self):
        """过滤到 0B OBJECT 行后，READ_OBJECT 仍然应报错"""
        session = object_get_session()

        object_create_table("CREATE TABLE sql_zero_filter(device STRING TAG, obj_col OBJECT FIELD)")

        session.execute_non_query_statement(
            "INSERT INTO sql_zero_filter(time, device, obj_col) VALUES(1, 'd1', to_object(true, 0, X'1234'))"
        )
        session.execute_non_query_statement(
            "INSERT INTO sql_zero_filter(time, device, obj_col) VALUES(2, 'd2', to_object(true, 0, X''))"
        )

        assert_read_object_fails(
            "sql_zero_filter",
            "time = 2",
            expected_message_parts=["offset", "object size 0"],
        )

    def test_06_select_still_returns_object_when_device_is_null(self):
        """device 为 SQL NULL 时，SELECT 仍应返回该行且 OBJECT 可读"""
        session = object_get_session()

        object_create_table("CREATE TABLE sql_device_null(device STRING TAG, obj_col OBJECT FIELD)")

        session.execute_non_query_statement(
            "INSERT INTO sql_device_null(time, device, obj_col) VALUES(1, null, to_object(true, 0, X'0102'))"
        )

        rows = fetch_query_rows(
            "SELECT * FROM sql_device_null ORDER BY time",
        )
        assert len(rows) == 1
        assert str(rows[0][1]).lower() in ("none", "null", "")
        assert rows[0][2] not in (None, "", "null", "None")

        display_rows = fetch_query_rows(
            "SELECT device, obj_col FROM sql_device_null ORDER BY time",
        )
        assert len(display_rows) == 1
        assert str(display_rows[0][0]).lower() in ("none", "null", "")
        assert display_rows[0][1] not in (None, "", "null", "None")

        assert read_object_content("sql_device_null") == [b"\x01\x02"]

    def test_07_select_still_returns_object_when_device_is_empty_string(self):
        """device 为空字符串时，SELECT 仍应返回该行且 OBJECT 可读"""
        session = object_get_session()

        object_create_table("CREATE TABLE sql_device_empty(device STRING TAG, obj_col OBJECT FIELD)")

        session.execute_non_query_statement(
            "INSERT INTO sql_device_empty(time, device, obj_col) VALUES(1, '', to_object(true, 0, X'0304'))"
        )

        rows = fetch_query_rows(
            "SELECT * FROM sql_device_empty ORDER BY time",
        )
        assert len(rows) == 1
        assert rows[0][1] == ""
        assert rows[0][2] not in (None, "", "null", "None")

        display_rows = fetch_query_rows(
            "SELECT device, obj_col FROM sql_device_empty ORDER BY time",
        )
        assert len(display_rows) == 1
        assert display_rows[0][0] == ""
        assert display_rows[0][1] not in (None, "", "null", "None")

        assert read_object_content("sql_device_empty") == [b"\x03\x04"]


# ============================================================================
# 分段写入测试
# ============================================================================

class TestSegmentedWrite(ObjectSessionTestBase):
    """分段写入测试"""

    def setup_method(self, method):
        object_cleanup_tables()

    def teardown_method(self, method):
        object_cleanup_tables()

    def test_01_complete_write(self):
        """完整写入 (is_eof=true, offset=0)"""
        session = object_get_session()

        object_create_table("CREATE TABLE seg_full(device STRING TAG, obj_col OBJECT FIELD)")

        payload = b"\xca\xfe\xba\xbe"

        tablet = Tablet("seg_full",
            ["device", "obj_col"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["d1", None]], [1],
            [ColumnType.TAG, ColumnType.FIELD])

        tablet.add_value_object(0, 1, True, 0, payload)
        session.insert(tablet)

        result = read_object_content("seg_full")
        print(f"    Complete write: {result[0].hex()}")
        assert result[0] == payload

    def test_02_first_segment(self):
        """第一段 (is_eof=false)"""
        session = object_get_session()

        object_create_table("CREATE TABLE seg_first(device STRING TAG, obj_col OBJECT FIELD)")

        tablet = Tablet("seg_first",
            ["device", "obj_col"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["d1", None]], [1],
            [ColumnType.TAG, ColumnType.FIELD])

        tablet.add_value_object(0, 1, False, 0, b"\xca\xfe")
        session.insert(tablet)

        display = read_object_display("seg_first")
        print(f"    First segment display: {display}")
        assert len(display) == 1
        assert_object_display(display[0], expect_null=True)
        rows = fetch_query_rows(
            "SELECT READ_OBJECT(obj_col) FROM seg_first",
            value_getter="object",
        )
        print(f"    First segment READ_OBJECT rows: {rows}")
        assert len(rows) == 1
        assert rows[0][0] in (None, b"", "")

    def test_03_segmented_write_complete(self):
        """合法分段写入，最终可读"""
        session = object_get_session()

        object_create_table("CREATE TABLE seg_done(device STRING TAG, obj_col OBJECT FIELD)")

        session.execute_non_query_statement(
            "INSERT INTO seg_done(time, device, obj_col) VALUES(1, 'd1', to_object(false, 0, X'696F'))"
        )
        session.execute_non_query_statement(
            "INSERT INTO seg_done(time, device, obj_col) VALUES(1, 'd1', to_object(false, 2, X'7464'))"
        )
        session.execute_non_query_statement(
            "INSERT INTO seg_done(time, device, obj_col) VALUES(1, 'd1', to_object(true, 4, X'62'))"
        )

        display = read_object_display("seg_done")
        assert len(display) == 1
        assert_object_display(display[0])
        assert read_object_content("seg_done") == [b"iotdb"]

    def test_04_segmented_write_offset_gap(self):
        """分段 offset 不连续时报错"""
        session = object_get_session()

        object_create_table("CREATE TABLE seg_gap(device STRING TAG, obj_col OBJECT FIELD)")

        session.execute_non_query_statement(
            "INSERT INTO seg_gap(time, device, obj_col) VALUES(1, 'd1', to_object(false, 0, X'696F'))"
        )

        with pytest.raises(Exception) as excinfo:
            session.execute_non_query_statement(
                "INSERT INTO seg_gap(time, device, obj_col) VALUES(1, 'd1', to_object(true, 4, X'62'))"
            )
        print(f"    Segmented offset gap error: {excinfo.value}")

    def test_05_segmented_write_offset_zero_resets(self):
        """offset=0 会清除之前片段并重写"""
        session = object_get_session()

        object_create_table("CREATE TABLE seg_reset(device STRING TAG, obj_col OBJECT FIELD)")

        session.execute_non_query_statement(
            "INSERT INTO seg_reset(time, device, obj_col) VALUES(1, 'd1', to_object(false, 0, X'696F'))"
        )
        session.execute_non_query_statement(
            "INSERT INTO seg_reset(time, device, obj_col) VALUES(1, 'd1', to_object(true, 0, X'62'))"
        )

        display = read_object_display("seg_reset")
        assert len(display) == 1
        assert_object_display(display[0])
        assert read_object_content("seg_reset") == [b"b"]


# ============================================================================
# Payload 大小测试
# ============================================================================

class TestPayloadSizes(ObjectSessionTestBase):
    """不同大小 payload"""

    def setup_method(self, method):
        object_cleanup_tables()

    def teardown_method(self, method):
        object_cleanup_tables()

    def write_and_verify(self, table_name, payload):
        """通用写入验证"""
        session = object_get_session()

        object_create_table(f"CREATE TABLE {table_name}(device STRING TAG, obj_col OBJECT FIELD)")

        tablet = Tablet(table_name,
            ["device", "obj_col"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["d1", None]], [1],
            [ColumnType.TAG, ColumnType.FIELD])

        tablet.add_value_object(0, 1, True, 0, payload)
        session.insert(tablet)

        result = read_object_content(table_name)
        print(f"    Size {len(payload)} bytes: Expected len={len(payload)}, Got len={len(result[0]) if result[0] else 0}")
        assert result[0] == payload

    def test_01_bytes(self):
        """8 bytes"""
        self.write_and_verify("size_8b", b"\x01\x02\x03\x04\x05\x06\x07\x08")

    def test_02_kb(self):
        """1 KB"""
        self.write_and_verify("size_1kb", b"x" * 1024)

    def test_03_10kb(self):
        """10 KB"""
        self.write_and_verify("size_10kb", b"x" * 10240)

    def test_04_100kb(self):
        """100 KB"""
        self.write_and_verify("size_100kb", b"x" * 102400)

    def test_05_mb(self):
        """1 MB"""
        self.write_and_verify("size_1mb", b"x" * (1024 * 1024))


# ============================================================================
# 查询测试
# ============================================================================

class TestQueryMethods(ObjectSessionTestBase):
    """查询方法测试"""

    def setup_method(self, method):
        object_cleanup_tables()

    def teardown_method(self, method):
        object_cleanup_tables()

    def test_01_read_object(self):
        """READ_OBJECT 函数"""
        session = object_get_session()

        object_create_table("CREATE TABLE q_read(device STRING TAG, obj_col OBJECT FIELD)")

        payload = b"\xca\xfe\xba\xbe\xde\xad\xbe\xef"

        tablet = Tablet("q_read",
            ["device", "obj_col"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["d1", None]], [1],
            [ColumnType.TAG, ColumnType.FIELD])

        tablet.add_value_object(0, 1, True, 0, payload)
        session.insert(tablet)

        result = read_object_content("q_read")
        print(f"    READ_OBJECT: {result[0].hex()}")
        assert result[0] == payload

    def test_02_mixed_query(self):
        """混合列查询"""
        session = object_get_session()

        object_create_table("CREATE TABLE q_mixed(device STRING TAG, i INT32 FIELD, obj OBJECT FIELD)")

        payload = b"test"

        tablet = Tablet("q_mixed",
            ["device", "i", "obj"],
            [TSDataType.STRING, TSDataType.INT32, TSDataType.OBJECT],
            [["d1", 100, None]], [1],
            [ColumnType.TAG, ColumnType.FIELD, ColumnType.FIELD])

        tablet.add_value_object(0, 2, True, 0, payload)
        session.insert(tablet)

        with session.execute_query_statement(
            "SELECT i, READ_OBJECT(obj) FROM q_mixed"
        ) as result:
            row = result.next()
            fields = row.get_fields()
            print(f"    i={fields[0].get_int_value()}, obj={fields[1].get_binary_value()}")
            assert fields[0].get_int_value() == 100
            assert fields[1].get_binary_value() == payload


# ============================================================================
# 函数矩阵测试
# ============================================================================

class TestObjectFunctions(ObjectSessionTestBase):
    """OBJECT 相关函数测试，覆盖需求分析中的函数矩阵。"""

    def setup_method(self, method):
        object_cleanup_tables()

    def teardown_method(self, method):
        object_cleanup_tables()

    def prepare_function_table(self):
        session = object_get_session()
        object_create_table(
            "CREATE TABLE func_obj(device STRING TAG, obj_col OBJECT FIELD, obj_col2 OBJECT FIELD)"
        )
        session.execute_non_query_statement(
            "INSERT INTO func_obj(time, device, obj_col, obj_col2) "
            "VALUES(1, 'd1', to_object(true, 0, X'0102030405'), null)"
        )
        session.execute_non_query_statement(
            "INSERT INTO func_obj(time, device, obj_col, obj_col2) "
            "VALUES(2, 'd1', null, to_object(true, 0, X'AA'))"
        )
        session.execute_non_query_statement(
            "INSERT INTO func_obj(time, device, obj_col, obj_col2) "
            "VALUES(3, 'd1', to_object(true, 0, X''), to_object(true, 0, X'BBCC'))"
        )

    def test_01_read_object_default(self):
        self.prepare_function_table()
        rows = fetch_query_rows(
            "SELECT READ_OBJECT(obj_col) FROM func_obj WHERE time = 1", "binary"
        )
        assert rows == [[b"\x01\x02\x03\x04\x05"]]

    def test_02_read_object_with_offset_and_length(self):
        self.prepare_function_table()
        rows = fetch_query_rows(
            "SELECT READ_OBJECT(obj_col, 1, 3) FROM func_obj WHERE time = 1", "binary"
        )
        assert rows == [[b"\x02\x03\x04"]]

    def test_03_read_object_length_larger_than_remaining(self):
        self.prepare_function_table()
        rows = fetch_query_rows(
            "SELECT READ_OBJECT(obj_col, 3, 100) FROM func_obj WHERE time = 1", "binary"
        )
        assert rows == [[b"\x04\x05"]]

    def test_04_read_object_negative_length_reads_remaining(self):
        self.prepare_function_table()
        rows = fetch_query_rows(
            "SELECT READ_OBJECT(obj_col, 2, -1) FROM func_obj WHERE time = 1", "binary"
        )
        assert rows == [[b"\x03\x04\x05"]]

    def test_05_read_object_negative_offset_fails(self):
        self.prepare_function_table()
        assert_query_fails(
            "SELECT READ_OBJECT(obj_col, -1, 1) FROM func_obj WHERE time = 1",
            expected_message_parts=["offset"],
        )

    def test_06_read_object_offset_equal_to_size_fails(self):
        self.prepare_function_table()
        assert_query_fails(
            "SELECT READ_OBJECT(obj_col, 5, 1) FROM func_obj WHERE time = 1",
            expected_message_parts=["offset"],
        )

    def test_07_read_object_zero_byte_object_fails(self):
        self.prepare_function_table()
        assert_query_fails(
            "SELECT READ_OBJECT(obj_col) FROM func_obj WHERE time = 3",
            expected_message_parts=["offset", "object size 0"],
        )

    def test_08_length_function(self):
        self.prepare_function_table()
        rows = fetch_query_rows(
            "SELECT LENGTH(obj_col), LENGTH(obj_col2) FROM func_obj WHERE time = 1",
            "object",
        )
        assert rows == [[5, None]]

    def test_09_length_for_zero_byte_object(self):
        self.prepare_function_table()
        rows = fetch_query_rows(
            "SELECT LENGTH(obj_col), LENGTH(obj_col2) FROM func_obj WHERE time = 3",
            "object",
        )
        assert rows[0][0] == 0
        assert rows[0][1] == 2

    def test_10_cast_object_as_string(self):
        self.prepare_function_table()
        rows = fetch_query_rows(
            "SELECT CAST(obj_col AS STRING) FROM func_obj WHERE time = 1"
        )
        assert len(rows) == 1
        assert rows[0][0] is not None

    def test_11_try_cast_object_as_string(self):
        self.prepare_function_table()
        rows = fetch_query_rows(
            "SELECT TRY_CAST(obj_col AS STRING) FROM func_obj WHERE time = 1"
        )
        assert len(rows) == 1
        assert rows[0][0] is not None

    def test_12_coalesce_object(self):
        self.prepare_function_table()
        rows = fetch_query_rows(
            "SELECT READ_OBJECT(coalesce(obj_col, obj_col2)) FROM func_obj WHERE time = 2",
            "binary",
        )
        assert rows == [[b"\xAA"]]

    def test_13_is_null_and_is_not_null(self):
        self.prepare_function_table()
        rows = fetch_query_rows(
            "SELECT COUNT(*) FROM func_obj WHERE obj_col IS NULL", "object"
        )
        assert rows == [[1]]

        rows = fetch_query_rows(
            "SELECT COUNT(*) FROM func_obj WHERE obj_col IS NOT NULL", "object"
        )
        assert rows == [[2]]


# ============================================================================
# 错误处理测试
# ============================================================================

class TestErrorHandling:
    """错误处理测试"""

    def test_wrong_column_type(self):
        """非 OBJECT 列"""
        tablet = Tablet("t", ["id", "str"], [TSDataType.STRING, TSDataType.STRING], [["x", "y"]], [1])

        with pytest.raises(TypeError) as excinfo:
            tablet.add_value_object(0, 1, True, 0, b"data")
        print(f"    Got expected TypeError: {excinfo.value}")

    def test_row_index_out_of_range(self):
        """索引越界"""
        tablet = Tablet("t",
            ["device", "obj"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["d1", None]], [1],
            [ColumnType.TAG, ColumnType.FIELD])

        with pytest.raises(IndexError) as excinfo:
            tablet.add_value_object(10, 1, True, 0, b"data")
        print(f"    Got expected IndexError: {excinfo.value}")

    def test_column_name_not_found(self):
        """列名不存在"""
        tablet = Tablet("t",
            ["device", "obj"],
            [TSDataType.STRING, TSDataType.OBJECT],
            [["d1", None]], [1],
            [ColumnType.TAG, ColumnType.FIELD])

        with pytest.raises(KeyError) as excinfo:
            tablet.add_value_object_by_name("nonexistent", 0, True, 0, b"data")
        print(f"    Got expected KeyError: {excinfo.value}")


# ===== merged from test_utils_edge_branches.py =====
from datetime import date
from types import SimpleNamespace

import numpy as np
import pytest

from iotdb.utils.BitMap import BitMap
from iotdb.utils.Field import Field
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import ColumnType, Tablet


def test_bitmap_mark_and_is_all_unmarked():
    empty = BitMap(16)
    assert empty.is_all_unmarked() is True

    first_byte = BitMap(16)
    first_byte.mark(0)
    assert first_byte.is_all_unmarked() is False

    last_byte = BitMap(9)
    last_byte.mark(8)
    assert last_byte.is_all_unmarked() is False


def test_field_additional_branches():
    copied = Field.copy(Field(TSDataType.OBJECT, b"payload"))
    assert copied.get_string_value() == "payload"

    assert Field(TSDataType.BOOLEAN, True).get_string_value() == "True"
    assert Field(TSDataType.BLOB, b"\x01").get_string_value() == "0x1"
    assert int(Field(TSDataType.INT32, 5).get_object_value(TSDataType.INT32)) == 5
    assert float(Field(TSDataType.FLOAT, 1.5).get_object_value(TSDataType.FLOAT)) == pytest.approx(1.5)
    assert float(Field(TSDataType.DOUBLE, 2.5).get_object_value(TSDataType.DOUBLE)) == pytest.approx(2.5)
    assert Field(TSDataType.TEXT, b"abc").get_binary_value() == b"abc"
    assert Field(TSDataType.STRING, b"xyz").get_binary_value() == b"xyz"
    assert Field(TSDataType.TIMESTAMP, 1000, timezone="UTC", precision="ms").get_string_value().startswith(
        "1970-01-01T00:00:01"
    )
    assert str(Field(TSDataType.DATE, 20240417).get_object_value(TSDataType.DATE)).startswith("2024-04-17")
    assert int(Field(TSDataType.INT64, 7).get_long_value()) == 7


def test_tablet_bitmap_and_error_branches():
    with pytest.raises(RuntimeError):
        Tablet("root.sg.d1", ["a"], [TSDataType.INT32], [[1, 2]], [1, 2])

    tablet = Tablet(
        "root.sg.d1",
        ["flag", "count", "text", "obj", "day"],
        [
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.OBJECT,
            TSDataType.DATE,
        ],
        [
            [True, 10, "a", b"", date(2024, 4, 16)],
            [None, None, "b", None, date(2024, 4, 17)],
        ],
        [2, 1],
        [ColumnType.FIELD] * 5,
    )
    tablet.add_value_object(0, 3, True, 0, b"first")
    assert isinstance(tablet.get_binary_values(), bytes)

    bad = Tablet("root.sg.d1", ["obj"], [object()], [[b"x"]], [1])
    with pytest.raises(RuntimeError):
        bad.get_binary_values()


def test_numpy_tablet_validation_and_branches():
    class FakeType:
        def np_dtype(self):
            return np.dtype("O")

    with pytest.raises(RuntimeError):
        NumpyTablet(
            "root.sg.d1",
            ["a"],
            [TSDataType.INT32],
            [np.array([1, 2], dtype=np.int32)],
            np.array([1], dtype=np.int64),
        )

    with pytest.raises(RuntimeError):
        NumpyTablet(
            "root.sg.d1",
            ["a", "b"],
            [TSDataType.INT32],
            [
                np.array([1, 2], dtype=np.int32),
                np.array([b"x", b"y"], dtype=object),
            ],
            np.array([1, 2], dtype=np.int64),
        )

    timestamps = np.array([3, 1], dtype=np.int64)
    values = [
        np.array([30, 10], dtype=np.int32),
        np.array([b"old", b"new"], dtype=object),
        np.array([date(2024, 4, 16), date(2024, 4, 17)], dtype=object),
    ]
    bitmaps = [None, BitMap(2), BitMap(2)]
    bitmaps[1].mark(1)

    tablet = NumpyTablet(
        "root.sg.d1",
        ["count", "obj", "day"],
        [TSDataType.INT32, TSDataType.OBJECT, TSDataType.DATE],
        values,
        timestamps,
        bitmaps=bitmaps,
        column_types=[ColumnType.FIELD, ColumnType.FIELD, ColumnType.FIELD],
    )

    tablet.mark_none_value(1, 0)
    assert tablet.get_measurements() == ["count", "obj", "day"]
    assert tablet.get_row_number() == 2
    assert tablet.get_insert_target_name() == "root.sg.d1"
    assert list(tablet.get_timestamps()) == [1, 3]
    assert isinstance(tablet.get_binary_values(), bytes)

    bad_object = NumpyTablet(
        "root.sg.d1",
        ["obj"],
        [TSDataType.OBJECT],
        [np.array(["not-bytes", "still-not"], dtype=object)],
        np.array([1, 2], dtype=np.int64),
    )
    with pytest.raises(TypeError):
        bad_object.get_binary_values()

    bad_type = NumpyTablet(
        "root.sg.d1",
        ["x"],
        [FakeType()],
        [np.array([1], dtype=object)],
        np.array([1], dtype=np.int64),
    )
    with pytest.raises(RuntimeError):
        bad_type.get_binary_values()

# ===== merged from test_utils_tablet_numpytablet.py =====
from datetime import date

import numpy as np
import pytest

from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import ColumnType, Tablet
from iotdb.utils.object_column import decode_object_cell


def test_tablet_object_write_and_serialization():
    tablet = Tablet(
        "root.sg.d1",
        ["flag", "count", "text", "obj", "day"],
        [
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.OBJECT,
            TSDataType.DATE,
        ],
        [
            [True, 10, "a", b"", date(2024, 4, 16)],
            [None, None, "b", b"", date(2024, 4, 17)],
        ],
        [2, 1],
        [ColumnType.FIELD] * 5,
    )

    tablet.add_value_object(0, 3, True, 0, b"first")
    tablet.add_value_object_by_name("obj", 1, False, 9, b"second")

    assert tablet.get_timestamps() == [1, 2]
    assert decode_object_cell(tablet.get_values()[0][3]) == (True, 0, b"first")
    assert decode_object_cell(tablet.get_values()[1][3]) == (False, 9, b"second")
    assert isinstance(tablet.get_binary_timestamps(), bytes)
    assert isinstance(tablet.get_binary_values(), bytes)


def test_tablet_object_validation_errors():
    tablet = Tablet(
        "root.sg.d1",
        ["flag", "obj"],
        [TSDataType.BOOLEAN, TSDataType.OBJECT],
        [[True, b""], [False, b""]],
        [1, 2],
    )

    with pytest.raises(TypeError):
        tablet.add_value_object(0, 0, True, 0, b"bad")
    with pytest.raises(IndexError):
        tablet.add_value_object(-1, 1, True, 0, b"bad")
    with pytest.raises(IndexError):
        tablet.add_value_object(0, 9, True, 0, b"bad")
    with pytest.raises(KeyError):
        tablet.add_value_object_by_name("missing", 0, True, 0, b"bad")


def test_numpy_tablet_object_and_bitmap_paths():
    timestamps = np.array([3, 1], dtype=np.int64)
    values = [
        np.array([30, 10], dtype=np.int32),
        np.array([b"old", b"new"], dtype=object),
    ]
    bitmaps = [None, BitMap(2)]
    bitmaps[1].mark(1)

    tablet = NumpyTablet(
        "root.sg.d1",
        ["count", "obj"],
        [TSDataType.INT32, TSDataType.OBJECT],
        values,
        timestamps,
        bitmaps=bitmaps,
    )

    assert list(tablet.get_timestamps()) == [1, 3]
    tablet.add_value_object(0, 1, True, 5, b"payload")
    assert decode_object_cell(tablet.get_values()[1][0]) == (True, 5, b"payload")
    assert isinstance(tablet.get_binary_timestamps(), bytes)
    assert isinstance(tablet.get_binary_values(), bytes)

    with pytest.raises(TypeError):
        tablet.add_value_object(0, 0, True, 0, b"bad")


# ===== merged from test_object_column_category.py / test_object_restrict_limit_false.py =====
MERGED_OBJECT_CATEGORY_DB = "test_object_category"
MERGED_OBJECT_RESTRICT_DB = "test_object_limit_false"
MERGED_OBJECT_TABLE_CASES = [
    "tbl_ok_basic",
    "tbl.dot",
    "tbl..dot",
    "tbl./slash",
    r"tbl.\backslash",
]
MERGED_OBJECT_FIELD_CASES = [
    "obj_col",
    "obj.col",
    "obj..col",
    "obj./col",
    r"obj.\col",
]
MERGED_OBJECT_DEVICE_CASES = [
    "d1",
    ".",
    "..",
    "a./b",
    r"a.\b",
]


def merged_quote_identifier(name):
    return '"' + name.replace('"', '""') + '"'


def merged_make_table_session():
    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    return TableSession(
        TableSessionConfig(
            node_urls=[f"{config['host']}:{config['port']}"],
            username=config["username"],
            password=config["password"],
        )
    )


@pytest.fixture()
def merged_object_case_session():
    session = merged_make_table_session()
    try:
        for database_name in [MERGED_OBJECT_CATEGORY_DB, MERGED_OBJECT_RESTRICT_DB]:
            try:
                session.execute_non_query_statement(f"drop database {database_name}")
            except Exception:
                pass
            session.execute_non_query_statement(f"create database {database_name}")
        yield session
    finally:
        for database_name in [MERGED_OBJECT_CATEGORY_DB, MERGED_OBJECT_RESTRICT_DB]:
            try:
                session.execute_non_query_statement(f"drop database {database_name}")
            except Exception:
                pass
        session.close()


def test_object_column_category_cases(merged_object_case_session):
    session = merged_object_case_session
    session.execute_non_query_statement(f"use {MERGED_OBJECT_CATEGORY_DB}")
    session.execute_non_query_statement(
        "create table single_field(device STRING TAG, obj_col OBJECT FIELD)"
    )
    session.execute_non_query_statement(
        "insert into single_field(time, device, obj_col) values(1, 'd1', to_object(false, 0, X'01'))"
    )
    session.execute_non_query_statement(
        "create table multi_field(device STRING TAG, obj1 OBJECT FIELD, obj2 OBJECT FIELD)"
    )
    session.execute_non_query_statement(
        "insert into multi_field(time, device, obj1, obj2) "
        "values(1, 'd1', to_object(false, 0, X'01'), to_object(false, 0, X'0203'))"
    )
    with pytest.raises(Exception):
        session.execute_non_query_statement(
            "create table object_tag(obj_col OBJECT TAG, s STRING FIELD)"
        )
    with pytest.raises(Exception):
        session.execute_non_query_statement(
            "create table object_attr(device STRING TAG, obj_col OBJECT ATTRIBUTE, s STRING FIELD)"
        )


def test_object_restrict_limit_false_cases(merged_object_case_session):
    session = merged_object_case_session
    session.execute_non_query_statement(f"use {MERGED_OBJECT_RESTRICT_DB}")

    for table_name in MERGED_OBJECT_TABLE_CASES:
        session.execute_non_query_statement(
            f"create table {merged_quote_identifier(table_name)}("
            f"{merged_quote_identifier('device')} STRING TAG, "
            f"{merged_quote_identifier('obj_col')} OBJECT FIELD)"
        )

    for index, field_name in enumerate(MERGED_OBJECT_FIELD_CASES, start=1):
        table_name = f"tbl_field_case_{index}"
        session.execute_non_query_statement(
            f"create table {merged_quote_identifier(table_name)}("
            f"{merged_quote_identifier('device')} STRING TAG, "
            f"{merged_quote_identifier(field_name)} OBJECT FIELD)"
        )

    for index, device_name in enumerate(MERGED_OBJECT_DEVICE_CASES, start=1):
        table_name = f"tbl_device_case_{index}"
        session.execute_non_query_statement(
            f"create table {merged_quote_identifier(table_name)}("
            f"{merged_quote_identifier('device')} STRING TAG, "
            f"{merged_quote_identifier('obj_col')} OBJECT FIELD)"
        )
        session.execute_non_query_statement(
            f"insert into {merged_quote_identifier(table_name)}(time, {merged_quote_identifier('device')}, {merged_quote_identifier('obj_col')}) "
            f"values(1, '{device_name}', to_object(false, 0, X'01'))"
        )

