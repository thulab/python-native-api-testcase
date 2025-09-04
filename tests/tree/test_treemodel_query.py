import numpy as np
import pytest
import yaml
import os
from datetime import date
from iotdb.Session import Session
from iotdb.SessionPool import SessionPool, PoolConfig
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from iotdb.utils.NumpyTablet import NumpyTablet
from datetime import date

from iotdb.utils.exception import IoTDBConnectionException

"""
 Title：测试树模型python查询接口
 Describe：树模型
 Author：肖林捷
 Date：2024/12/10
"""

config_path = "../conf/config.yml"
group_name1 = "root.test_query.db1"
group_name2 = "root.test_query.db2"
device_id1 = group_name1 + ".d1"
device_id2 = group_name1 + ".d2"
measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING", ]
data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                 TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
ts_path_lst1 = [device_id1 + "." + measurements_lst[i] for i in range(len(measurements_lst))]
ts_path_lst2 = [device_id2 + "." + measurements_lst[i] for i in range(len(measurements_lst))]
encoding_lst = [TSEncoding.PLAIN for _ in range(len(measurements_lst))]
compressor_lst = [Compressor.LZ4 for _ in range(len(measurements_lst))]


def read_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


def get_session_():
    global session
    global session_pool
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    if config['enable_cluster']:
        session = Session(config['host'], config['port'], config['username'], config['password'])
        session.open()
        return session
    else:
        pool_config = PoolConfig(
            node_urls=config['node_urls'],
            user_name=config['username'],
            password=config['password'],
        )
        max_pool_size = 10
        wait_timeout_in_ms = 100000
        session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
        session = session_pool.get_session()
        session.open(False)
        return session


def create_database(session):
    # 创建数据库
    session.set_storage_group(group_name1)
    session.set_storage_group(group_name2)


def create_timeseries(session):
    for i in range(len(ts_path_lst1)):
        session.create_time_series(ts_path_lst1[i], data_type_lst[i], encoding_lst[i], compressor_lst[i])


def create_aligned_timeseries(session):
    session.create_aligned_time_series(device_id2, measurements_lst, data_type_lst, encoding_lst, compressor_lst)


def insert_data():
    global session
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    values_ = [
        [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
        [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz",
         -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
         "abcdefghijklmnopqrstuvwsyz"],
        [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
         9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
         "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
        [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
        [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1),
         '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
        [True, 10, 11, 4.123456, 4.123456789012345,
         "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11,
         date(1970, 1, 1),
         '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
             'utf-8'),
         "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
        [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'),
         "string01"],
        [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
        [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
        [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
    ]
    tablet_ = Tablet(device_id1, measurements_lst, data_type_lst, values_, timestamps_)
    session.insert_tablet(tablet_)

    # 2、Numpy Tablet（含空值）
    np_values_ = [
        np.array([False, True, False, True, False, True, False, True, False, False], TSDataType.BOOLEAN.np_dtype()),
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_bitmaps_ = []
    for i in range(len(measurements_lst)):
        np_bitmaps_.append(BitMap(len(np_timestamps_)))
    np_bitmaps_[0].mark(9)
    np_bitmaps_[1].mark(8)
    np_bitmaps_[2].mark(9)
    np_bitmaps_[4].mark(8)
    np_bitmaps_[5].mark(9)
    np_bitmaps_[6].mark(8)
    np_bitmaps_[7].mark(9)
    np_bitmaps_[8].mark(8)
    np_bitmaps_[9].mark(9)
    np_tablet_ = NumpyTablet(device_id2, measurements_lst, data_type_lst, np_values_, np_timestamps_, np_bitmaps_)
    session.insert_aligned_tablet(np_tablet_)


@pytest.fixture()
def fixture_():
    global session
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    # 用例执行前的环境搭建代码
    # 获取session
    session = get_session_()
    # 清理环境
    with session.execute_query_statement("show databases") as session_data_set:
        while session_data_set.has_next():
            fields = session_data_set.next().get_fields()
            if str(fields[0]) != "root.__system":
                session.execute_non_query_statement("delete database " + str(fields[0]))
    # 创建数据库
    create_database(session)
    # 创建时间序列
    create_timeseries(session)
    create_aligned_timeseries(session)
    # 写入数据
    insert_data()

    yield
    # 用例执行完成后清理环境代码
    try:
        with session.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "root.__system":
                    session.execute_non_query_statement("delete database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)
    if config['enable_cluster']:
        session.close()
    else:
        session_pool.put_back(session)
        session_pool.close()


# 1、测试 execute_query_statement 函数
@pytest.mark.usefixtures('fixture_')
def test_execute_query_statement():
    global session
    # 1、验证行数
    with session.execute_query_statement(
            "select * from " + device_id1
    ) as session_data_set:
        actual_num_ = 0
        while session_data_set.has_next():
            session_data_set.next()
            actual_num_ = actual_num_ + 1
        assert 10 == actual_num_, "The expected quantity is inconsistent with the actual quantity, expect: 10, actual: " + str(
            actual_num_)

    with session.execute_query_statement(
            "select * from " + device_id2
    ) as session_data_set:
        actual_num_ = 0
        while session_data_set.has_next():
            session_data_set.next()
            actual_num_ = actual_num_ + 1
        assert 10 == actual_num_, "The expected quantity is inconsistent with the actual quantity, expect: 10, actual: " + str(
            actual_num_)

    # 2、验证列名和数据类型
    with session.execute_query_statement(
            "select BOOLEAN, INT32, INT64, FLOAT,DOUBLE, TEXT, TS, DATE, BLOB, STRING from " + device_id1
    ) as session_data_set:
        for i in range(len(ts_path_lst1)):
            assert session_data_set.get_column_names()[i + 1] == ts_path_lst1[
                i], "The expected quantity is inconsistent with the actual quantity, expect: " + \
                        ts_path_lst1[i + 1] + ", actual: " + session_data_set.get_column_names()[i + 1]
        for i in range(len(data_type_lst)):
            assert session_data_set.get_column_types()[i + 1] == data_type_lst[
                i], "The expected quantity is inconsistent with the actual quantity, expect: " + str(
                data_type_lst[i]) + ", actual: " + str(session_data_set.get_column_types()[i + 1])

    with session.execute_query_statement(
            "select BOOLEAN, INT32, INT64, FLOAT,DOUBLE, TEXT, TS, DATE, BLOB, STRING from " + device_id2
    ) as session_data_set:
        for i in range(len(ts_path_lst2)):
            assert session_data_set.get_column_names()[i + 1] == ts_path_lst2[
                i], "The expected quantity is inconsistent with the actual quantity, expect: " + \
                        ts_path_lst2[i + 1] + ", actual: " + session_data_set.get_column_names()[i + 1]
        for i in range(len(data_type_lst)):
            assert session_data_set.get_column_types()[i + 1] == data_type_lst[
                i], "The expected quantity is inconsistent with the actual quantity, expect: " + str(
                data_type_lst[i]) + ", actual: " + str(session_data_set.get_column_types()[i + 1])

    # 3、验证数据
    with session.execute_query_statement(
            "select * from " + device_id1
    ) as session_data_set:
        while session_data_set.has_next():
            row_record = session_data_set.next()
            fields = row_record.get_fields()
            for i in range(len(measurements_lst)):
                if data_type_lst[i] == TSDataType.BOOLEAN:
                    fields[i].get_bool_value()
                elif data_type_lst[i] == TSDataType.INT32:
                    fields[i].get_int_value()
                elif data_type_lst[i] == TSDataType.INT64:
                    fields[i].get_long_value()
                elif data_type_lst[i] == TSDataType.FLOAT:
                    fields[i].get_float_value()
                elif data_type_lst[i] == TSDataType.DOUBLE:
                    fields[i].get_double_value()
                elif data_type_lst[i] == TSDataType.TEXT:
                    fields[i].get_string_value()
                elif data_type_lst[i] == TSDataType.DATE:
                    fields[i].get_date_value()
                elif data_type_lst[i] == TSDataType.TIMESTAMP:
                    fields[i].get_timestamp_value()
                elif data_type_lst[i] == TSDataType.BLOB:
                    fields[i].get_binary_value()
                elif data_type_lst[i] == TSDataType.STRING:
                    fields[i].get_string_value()
                else:
                    assert False, "The expected quantity is inconsistent with the actual quantity, expect: " + str(
                        data_type_lst[i]) + ", actual: " + str(fields[i].get_data_type())

    # 4、其他函数
    with session.execute_query_statement(
            "select * from " + device_id1
    ) as session_data_set:
        session_data_set.set_fetch_size(1024)
        assert 1024 == session_data_set.get_fetch_size(), "The expected quantity is inconsistent with the actual quantity, expect: 1024, actual: " + str(
            session_data_set.get_fetch_size())
        while session_data_set.has_next():
            session_data_set.next()
        session_data_set.todf()
        session_data_set.close_operation_handle()


# 2、测试 execute_raw_data_query 函数
@pytest.mark.usefixtures('fixture_')
def test_execute_raw_data_query():
    global session
    # 1、验证行数
    with session.execute_raw_data_query(
            ts_path_lst1, -9223372036854775808, 9223372036854775807
    ) as session_data_set:
        actual_num_ = 0
        while session_data_set.has_next():
            session_data_set.next()
            actual_num_ = actual_num_ + 1
        assert 10 == actual_num_, "The expected quantity is inconsistent with the actual quantity, expect: 10, actual: " + str(
            actual_num_)

    with session.execute_raw_data_query(
            ts_path_lst2, -9223372036854775808, 9223372036854775807
    ) as session_data_set:
        actual_num_ = 0
        while session_data_set.has_next():
            session_data_set.next()
            actual_num_ = actual_num_ + 1
        assert 10 == actual_num_, "The expected quantity is inconsistent with the actual quantity, expect: 10, actual: " + str(
            actual_num_)


# 3、测试 execute_last_data_query 函数
@pytest.mark.usefixtures('fixture_')
def test_execute_last_data_query():
    global session
    # 1、验证行数
    with session.execute_last_data_query(
            ts_path_lst1, 9223372036854775807
    ) as session_data_set:
        while session_data_set.has_next():
            session_data_set.next()

################## 异常情况 ##################

