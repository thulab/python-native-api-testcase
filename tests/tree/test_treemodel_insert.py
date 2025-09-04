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

"""
 Title：测试树模型python写入接口
 Describe：树模型，测试各种写入接口
 Author：肖林捷
 Date：2024/10/24
 TODO：补充完善各种编码和压缩之间是否会导致数据不一致
"""

# 配置文件目录
config_path = "../conf/config.yml"


# 获取配置文件的目录
def read_config():
    with open(config_path, 'r', encoding='utf-8') as file:
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
    session.set_storage_group("root.tests.g1")
    session.set_storage_group("root.tests.g2")
    session.set_storage_group("root.tests.g3")
    session.set_storage_group("root.tests.g4")
    session.set_storage_group("root.tests.g5")
    session.set_storage_group("root.tests.g6")


def create_timeseries(session):
    # 1、创建单个时间序列
    session.create_time_series("root.tests.g5.d1.STRING1", TSDataType.STRING, TSEncoding.DICTIONARY,
                               Compressor.UNCOMPRESSED,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.STRING2", TSDataType.STRING, TSEncoding.PLAIN, Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.STRING3", TSDataType.STRING, TSEncoding.DICTIONARY, Compressor.GZIP,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.STRING4", TSDataType.STRING, TSEncoding.PLAIN, Compressor.ZSTD,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.STRING5", TSDataType.STRING, TSEncoding.DICTIONARY, Compressor.LZMA2,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.STRING6", TSDataType.STRING, TSEncoding.PLAIN, Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)

    session.create_time_series("root.tests.g5.d1.TS1", TSDataType.TIMESTAMP, TSEncoding.PLAIN, Compressor.GZIP,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.TS2", TSDataType.TIMESTAMP, TSEncoding.RLE, Compressor.UNCOMPRESSED,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.TS3", TSDataType.TIMESTAMP, TSEncoding.TS_2DIFF, Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.TS4", TSDataType.TIMESTAMP, TSEncoding.ZIGZAG, Compressor.ZSTD,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.TS5", TSDataType.TIMESTAMP, TSEncoding.CHIMP, Compressor.LZMA2,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.TS6", TSDataType.TIMESTAMP, TSEncoding.SPRINTZ, Compressor.GZIP,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.TS7", TSDataType.TIMESTAMP, TSEncoding.RLBE, Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.TS8", TSDataType.TIMESTAMP, TSEncoding.GORILLA, Compressor.GZIP,
                               props=None, tags=None, attributes=None, alias=None)

    session.create_time_series("root.tests.g5.d1.DATE1", TSDataType.DATE, TSEncoding.PLAIN, Compressor.GZIP,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.DATE2", TSDataType.DATE, TSEncoding.RLE, Compressor.UNCOMPRESSED,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.DATE3", TSDataType.DATE, TSEncoding.TS_2DIFF, Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.DATE4", TSDataType.DATE, TSEncoding.ZIGZAG, Compressor.ZSTD,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.DATE5", TSDataType.DATE, TSEncoding.CHIMP, Compressor.LZMA2,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.DATE6", TSDataType.DATE, TSEncoding.SPRINTZ, Compressor.GZIP,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.DATE7", TSDataType.DATE, TSEncoding.RLBE, Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.DATE8", TSDataType.DATE, TSEncoding.GORILLA, Compressor.GZIP,
                               props=None, tags=None, attributes=None, alias=None)

    session.create_time_series("root.tests.g5.d1.BLOB1", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.UNCOMPRESSED,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.BLOB2", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.BLOB3", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.GZIP,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.BLOB4", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.ZSTD,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.BLOB5", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.LZMA2,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series("root.tests.g5.d1.BLOB6", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)

    # 2、创建多个
    ts_path_lst = ["root.tests.g1.d1.BOOLEAN", "root.tests.g1.d1.INT32", "root.tests.g1.d1.INT64",
                   "root.tests.g1.d1.FLOAT", "root.tests.g1.d1.DOUBLE", "root.tests.g1.d1.TEXT", "root.tests.g1.d1.TS",
                   "root.tests.g1.d1.DATE", "root.tests.g1.d1.BLOB", "root.tests.g1.d1.STRING",
                   "root.tests.g1.d2.BOOLEAN", "root.tests.g1.d2.INT32", "root.tests.g1.d2.INT64",
                   "root.tests.g1.d2.FLOAT", "root.tests.g1.d2.DOUBLE", "root.tests.g1.d2.TEXT", "root.tests.g1.d2.TS",
                   "root.tests.g1.d2.DATE", "root.tests.g1.d2.BLOB", "root.tests.g1.d2.STRING",
                   "root.tests.g1.d3.BOOLEAN", "root.tests.g1.d3.INT32", "root.tests.g1.d3.INT64",
                   "root.tests.g1.d3.FLOAT", "root.tests.g1.d3.DOUBLE", "root.tests.g1.d3.TEXT", "root.tests.g1.d3.TS",
                   "root.tests.g1.d3.DATE", "root.tests.g1.d3.BLOB", "root.tests.g1.d3.STRING",
                   "root.tests.g2.d1.BOOLEAN", "root.tests.g2.d1.INT32", "root.tests.g2.d1.INT64",
                   "root.tests.g2.d1.FLOAT", "root.tests.g2.d1.DOUBLE", "root.tests.g2.d1.TEXT", "root.tests.g2.d1.TS",
                   "root.tests.g2.d1.DATE", "root.tests.g2.d1.BLOB", "root.tests.g2.d1.STRING"]
    data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                     TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,
                     TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                     TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,
                     TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                     TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,
                     TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                     TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ,
                    TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN,
                    TSEncoding.DICTIONARY,
                    TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ,
                    TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN,
                    TSEncoding.DICTIONARY,
                    TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ,
                    TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN,
                    TSEncoding.DICTIONARY,
                    TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ,
                    TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN,
                    TSEncoding.DICTIONARY]
    compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD,
                      Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,
                      Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD,
                      Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,
                      Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD,
                      Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,
                      Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD,
                      Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP]
    session.create_multi_time_series(
        ts_path_lst, data_type_lst, encoding_lst, compressor_lst,
        props_lst=None, tags_lst=None, attributes_lst=None, alias_lst=None
    )


def create_aligned_timeseries(session):
    device_id = "root.tests.g3.d1"
    measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING", ]
    data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                     TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING, ]
    encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ,
                    TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN,
                    TSEncoding.DICTIONARY, ]
    compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD,
                      Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, ]
    session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)

    device_id = "root.tests.g3.d2"
    measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING", ]
    data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                     TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING, ]
    encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ,
                    TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN,
                    TSEncoding.DICTIONARY, ]
    compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD,
                      Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, ]
    session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)

    device_id = "root.tests.g3.d3"
    measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING", ]
    data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                     TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING, ]
    encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ,
                    TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN,
                    TSEncoding.DICTIONARY, ]
    compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD,
                      Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, ]
    session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)

    device_id = "root.tests.g4.d1"
    measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING", ]
    data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                     TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING, ]
    encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ,
                    TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN,
                    TSEncoding.DICTIONARY, ]
    compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD,
                      Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP, ]
    session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)


def query(sql):
    actual = 0
    with session.execute_query_statement(
            sql
    ) as session_data_set:
        session_data_set.set_fetch_size(1024)
        while session_data_set.has_next():
            session_data_set.next()
            actual = actual + 1

    return actual


@pytest.fixture()
def fixture_():
    global session
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    # 用例执行前的环境搭建代码
    # 获取session
    session = get_session_()
    # 清理环境
    try:
        with session.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "root.__system":
                    session.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)
    # 创建数据库
    create_database(session)
    # 创建时间序列
    create_timeseries(session)
    create_aligned_timeseries(session)

    yield
    # 用例执行完成后清理环境代码
    try:
        with session.execute_query_statement("show databases") as session_data_set:
            while session_data_set.has_next():
                fields = session_data_set.next().get_fields()
                if str(fields[0]) != "root.__system":
                    session.execute_non_query_statement("drop database " + str(fields[0]))
    except Exception as e:
        assert False, str(e)
    if config['enable_cluster']:
        session.close()
    else:
        session_pool.put_back(session)
        session_pool.close()


# 测试往非对齐时间序列写入一条 tablet 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_tablet():
    global session
    # 1、普通 Tablet 插入
    expect = 10
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
    session.insert_tablet(tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    # 判断是否符合预期
    assert expect == actual

    # 2、Numpy Tablet（不含空值）
    expect = 10
    device_id = "root.tests.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    session.insert_tablet(np_tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d2")
    # 判断是否符合预期
    assert expect == actual

    # 3、Numpy Tablet（含空值）
    expect = 10
    device_id = "root.tests.g1.d3"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    for i in range(len(measurements_)):
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
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_, np_bitmaps_)
    session.insert_tablet(np_tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d3")
    # 判断是否符合预期
    assert expect == actual


# 测试往对齐时间序列写入一条 tablet 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_tablet():
    global session
    # 1、普通 Tablet
    expect = 10
    device_id = "root.tests.g3.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
    session.insert_aligned_tablet(tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    # 判断是否符合预期
    assert expect == actual

    # 2、Numpy Tablet
    expect = 10
    device_id = "root.tests.g3.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    session.insert_aligned_tablet(np_tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d2")
    # 判断是否符合预期
    assert expect == actual

    # 3、Numpy Tablet（含空值）
    expect = 10
    device_id = "root.tests.g3.d3"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    for i in range(len(measurements_)):
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
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_, np_bitmaps_)
    session.insert_aligned_tablet(np_tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d3")
    # 判断是否符合预期
    assert expect == actual


# 测试往非对齐时间序列写入多条 tablet 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_tablets():
    global session
    # 1、普通 Tablet
    expect = 40
    device1_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet1_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)
    device1_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    tablet2_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)

    device2_id = "root.tests.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet3_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    device2_id = "root.tests.g2.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet4_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    tablet_lst = [tablet1_, tablet2_, tablet3_, tablet4_]
    session.insert_tablets(tablet_lst)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    actual = actual + query("select * from root.tests.g1.d2")
    actual = actual + query("select * from root.tests.g2.d1")
    # 判断是否符合预期
    assert expect == actual

    # 2、Numpy Tablet
    expect = 40
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet1_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([11, 12, 13, 14, 15, 16, 17, 18, 19, 20], TSDataType.INT64.np_dtype())
    np_tablet2_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.tests.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet3_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.tests.g2.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet4_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    np_tablet_list = [np_tablet1_, np_tablet2_, np_tablet3_, np_tablet4_]
    session.insert_tablets(np_tablet_list)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    actual = actual + query("select * from root.tests.g1.d2")
    actual = actual + query("select * from root.tests.g2.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试往对齐时间序列写入多条 tablet 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_tablets():
    global session
    # 1、普通 Tablet
    expect = 40
    device1_id = "root.tests.g3.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet1_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)
    device1_id = "root.tests.g3.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    tablet2_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)

    tablet_lst = [tablet1_, tablet2_]
    session.insert_aligned_tablets(tablet_lst)

    device2_id = "root.tests.g3.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet3_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    device2_id = "root.tests.g4.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet4_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    tablet_lst = [tablet1_, tablet2_, tablet3_, tablet4_]
    session.insert_aligned_tablets(tablet_lst)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    actual = actual + query("select * from root.tests.g3.d2")
    actual = actual + query("select * from root.tests.g4.d1")
    # 判断是否符合预期
    assert expect == actual

    # 2、Numpy Tablet
    expect = 40
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet1_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([11, 12, 13, 14, 15, 16, 17, 18, 19, 20], TSDataType.INT64.np_dtype())
    np_tablet2_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.tests.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet3_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.tests.g2.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet4_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    np_tablet_list = [np_tablet1_, np_tablet2_, np_tablet3_, np_tablet4_]
    session.insert_aligned_tablets(np_tablet_list)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    actual = actual + query("select * from root.tests.g3.d2")
    actual = actual + query("select * from root.tests.g4.d1")
    # 判断是否符合预期
    assert expect == actual


# # 测试往对齐时间序列写入多条 tablet 数据（无重定向）
# @pytest.mark.usefixtures('fixture_')
# def test_insert_aligned_tablets_close_redirection():
#     with open(config_path, 'r', encoding='utf-8') as file:
#         config = yaml.safe_load(file)
#     if config['enable_cluster']:
#         session1 = Session(config['host'], config['port'], config['username'], config['password'], enable_redirection=False)
#         session1.open()
#     else:
#         pool_config = PoolConfig(
#             node_urls=config['node_urls'],
#             user_name=config['username'],
#             password=config['password'],
#             enable_redirection=False
#         )
#         max_pool_size = 10
#         wait_timeout_in_ms = 100000
#         session_pool1 = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
#         session1 = session_pool.get_session()
#         session1.open()
#     # 1、普通 Tablet
#     expect = 20
#     device1_id = "root.tests.g3.d1"
#     measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
#     data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
#                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
#     values_ = [
#         [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
#         [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz",
#          -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
#          "abcdefghijklmnopqrstuvwsyz"],
#         [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
#          9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
#          "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
#         [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
#         [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1),
#          '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
#         [True, 10, 11, 4.123456, 4.123456789012345,
#          "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11,
#          date(1970, 1, 1),
#          '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
#              'utf-8'),
#          "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
#         [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'),
#          "string01"],
#         [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
#         [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
#         [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
#     ]
#     timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
#     tablet1_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)
#     device1_id = "root.tests.g3.d1"
#     measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
#     data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
#                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
#     values_ = [
#         [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
#         [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz",
#          -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
#          "abcdefghijklmnopqrstuvwsyz"],
#         [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
#          9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
#          "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
#         [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
#         [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1),
#          '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
#         [True, 10, 11, 4.123456, 4.123456789012345,
#          "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11,
#          date(1970, 1, 1),
#          '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
#              'utf-8'),
#          "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
#         [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'),
#          "string01"],
#         [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
#         [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
#         [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
#     ]
#     timestamps_ = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
#     tablet2_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)
#
#     tablet_lst = [tablet1_, tablet2_]
#     session1.insert_aligned_tablets(tablet_lst)
#     # 计算实际时间序列的行数量
#     actual = query("select * from root.tests.g3.d1")
#     # 判断是否符合预期
#     assert expect == actual



# 测试往非对齐时间序列写入一条 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_record():
    global session
    expect = 9
    session.insert_record("root.tests.g1.d1", 1,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'),
                           "1234567890"])
    session.insert_record("root.tests.g1.d1", 2,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567,
                           "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1),
                           'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"])
    session.insert_record("root.tests.g1.d1", 3,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567,
                           "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31),
                           '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
    session.insert_record("root.tests.g1.d1", 4,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"])
    session.insert_record("root.tests.g1.d1", 5,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11,
                           date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'),
                           "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
    session.insert_record("root.tests.g1.d1", 6,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, 10, 11, 4.123456, 4.123456789012345,
                           "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                           11, date(1970, 1, 1),
                           '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                               'utf-8'),
                           "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
    session.insert_record("root.tests.g1.d1", 7,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
                           'Hello, World!'.encode('utf-8'), "string01"])
    session.insert_record("root.tests.g1.d1", 8,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""])
    session.insert_record("root.tests.g1.d1", 9,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'),
                           "string01"])
    session.insert_record("root.tests.g1.d1", 10,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'),
                           "string01"])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试往对齐时间序列写入一条 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_record():
    global session
    expect = 9
    session.insert_aligned_record("root.tests.g3.d1", 1,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1),
                                   '1234567890'.encode('utf-8'), "1234567890"])
    session.insert_aligned_record("root.tests.g3.d1", 2,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567,
                                   "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1),
                                   'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"])
    session.insert_aligned_record("root.tests.g3.d1", 3,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567,
                                   "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31),
                                   '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
    session.insert_aligned_record("root.tests.g3.d1", 4,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'),
                                   "没问题"])
    session.insert_aligned_record("root.tests.g3.d1", 5,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11,
                                   date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'),
                                   "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
    session.insert_aligned_record("root.tests.g3.d1", 6,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, 10, 11, 4.123456, 4.123456789012345,
                                   "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                                   11, date(1970, 1, 1),
                                   '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                                       'utf-8'),
                                   "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
    session.insert_aligned_record("root.tests.g3.d1", 7,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
                                   'Hello, World!'.encode('utf-8'), "string01"])
    session.insert_aligned_record("root.tests.g1.d1", 8,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""])
    session.insert_aligned_record("root.tests.g3.d1", 9,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1),
                                   'Hello, World!'.encode('utf-8'), "string01"])
    session.insert_aligned_record("root.tests.g3.d1", 10,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1),
                                   'Hello, World!'.encode('utf-8'), "string01"])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试往非对齐时间序列写入多条 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_records():
    global session
    expect = 9
    session.insert_records(
        ["root.tests.g1.d1", "root.tests.g1.d2", "root.tests.g2.d1", "root.tests.g1.d1", "root.tests.g1.d1",
         "root.tests.g1.d1", "root.tests.g1.d1", "root.tests.g1.d1", "root.tests.g1.d1"],
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
        [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]],
        [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]],
        [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
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
         [None, None, None, None, None, "None", None, date(1970, 1, 1), 'None'.encode('utf-8'), "None"],
         [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
         [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    actual = actual + query("select * from root.tests.g1.d2")
    actual = actual + query("select * from root.tests.g2.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试往对齐时间序列写入多条 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_records():
    global session
    expect = 9
    session.insert_aligned_records(
        ["root.tests.g3.d1", "root.tests.g3.d2", "root.tests.g4.d1", "root.tests.g3.d1", "root.tests.g3.d1",
         "root.tests.g3.d1", "root.tests.g3.d1", "root.tests.g3.d1", "root.tests.g3.d1"],
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
        [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]],
        [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]],
        [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
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
         [None, None, None, None, None, "None", None, date(1970, 1, 1), 'None'.encode('utf-8'), "None"],
         [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
         [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    actual = actual + query("select * from root.tests.g3.d2")
    actual = actual + query("select * from root.tests.g4.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试插入同属于一个非对齐 device 的多个 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_records_of_one_device():
    global session
    #（1）测试插入各种数据类型和值
    expect = 9
    session.insert_records_of_one_device("root.tests.g1.d1",
                                         [1, 2, 3, 4, 5, 6, 7, 8, 9],
                                         [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"]],
                                         [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING]],
                                         [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1),
                                           '1234567890'.encode('utf-8'), "1234567890"],
                                          [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567,
                                           "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1),
                                           'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
                                          [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567,
                                           "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31),
                                           '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
                                           "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
                                          [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1),
                                           '没问题'.encode('utf-8'), "没问题"],
                                          [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                                           11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'),
                                           "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
                                          [True, 10, 11, 4.123456, 4.123456789012345,
                                           "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                                           11, date(1970, 1, 1),
                                           '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                                               'utf-8'),
                                           "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
                                          [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
                                           'Hello, World!'.encode('utf-8'), "string01"],
                                          [None, None, None, None, None, "None", None, date(1970, 1, 1),
                                           'None'.encode('utf-8'), "None"],
                                          [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1),
                                           'Hello, World!'.encode('utf-8'), "string01"],
                                          [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1),
                                           'Hello, World!'.encode('utf-8'), "string01"]])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试插入同属于一个对齐 device 的多个 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_records_of_one_device():
    global session
    expect = 9
    session.insert_aligned_records_of_one_device("root.tests.g3.d1",
                                                 [1, 2, 3, 4, 5, 6, 7, 8, 9],
                                                 [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"]],
                                                 [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING]],
                                                 [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1),
                                                   '1234567890'.encode('utf-8'), "1234567890"],
                                                  [True, -2147483648, -9223372036854775808, -0.12345678,
                                                   -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz",
                                                   -9223372036854775808, date(1000, 1, 1),
                                                   'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
                                                   "abcdefghijklmnopqrstuvwsyz"],
                                                  [True, 2147483647, 9223372036854775807, 0.123456789,
                                                   0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
                                                   9223372036854775807, date(9999, 12, 31),
                                                   '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
                                                   "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
                                                  [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1),
                                                   '没问题'.encode('utf-8'), "没问题"],
                                                  [True, -1, -1, 1.1234567, 1.1234567890123456,
                                                   "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1),
                                                   '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'),
                                                   "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
                                                  [True, 10, 11, 4.123456, 4.123456789012345,
                                                   "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                                                   11, date(1970, 1, 1),
                                                   '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                                                       'utf-8'),
                                                   "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
                                                  [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11,
                                                   date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
                                                  [None, None, None, None, None, "None", None, date(1970, 1, 1),
                                                   'None'.encode('utf-8'), "None"],
                                                  [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1),
                                                   'Hello, World!'.encode('utf-8'), "string01"],
                                                  [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1),
                                                   'Hello, World!'.encode('utf-8'), "string01"]])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试带有类型推断的写入：写入单条记录
@pytest.mark.usefixtures('fixture_')
def test_insert_str_record():
    global session
    expect1 = 1
    session.insert_str_record("root.tests.g5.d1",
                              1,
                              ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                              ["true", "10", "11", "11.11", "10011.1", "test01", "11", "1970-01-01", 'b"x12x34"',
                               "string01"])
    # 计算实际时间序列的行数量
    actual1 = query("select * from root.tests.g5.d1")
    # 判断是否符合预期
    assert expect1 == actual1

    expect2 = 1
    session.insert_str_record("root.tests.g5.d2",
                              1,
                              "BOOLEAN",
                              "true")
    # 计算实际时间序列的行数量
    actual2 = query("select * from root.tests.g5.d2")
    # 判断是否符合预期
    assert expect2 == actual2

    # TODO: 带None值对应位置的物理量会被删除导致，值和物理量数量不一致
    # expect3 = 1
    # session.insert_str_record("root.tests.g5.d3",
    #                           1,
    #                           ["BOOLEAN", "INT32", "INT64"],
    #                           ["true", None, "11"])
    # # 计算实际时间序列的行数量
    # actual3 = query("select * from root.tests.g5.d3")
    # # 判断是否符合预期
    # assert expect3 == actual3


# 测试带有类型推断的写入：写入单条对齐记录
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_str_record():
    global session
    expect1 = 1
    session.insert_aligned_str_record("root.tests.g5.d10",
                                      1,
                                      ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                       "STRING"],
                                      ["true", "10", "11", "11.11", "10011.1", "test01", "11", "1970-01-01",
                                       'b"x12x34"',
                                       "string01"])
    # 计算实际时间序列的行数量
    actual1 = query("select * from root.tests.g5.d10")
    # 判断是否符合预期
    assert expect1 == actual1

    expect2 = 1
    session.insert_aligned_str_record("root.tests.g5.d11",
                                      1,
                                      "BOOLEAN",
                                      "true")
    # 计算实际时间序列的行数量
    actual2 = query("select * from root.tests.g5.d11")
    # 判断是否符合预期
    assert expect2 == actual2

    # TODO: 带None值对应位置的物理量会被删除导致，值和物理量数量不一致
    # expect3 = 1
    # session.insert_aligned_str_record("root.tests.g5.d12",
    #                                   1,
    #                                   ["BOOLEAN", "INT32", "INT64"],
    #                                   ["true", None, "11"])
    # # 计算实际时间序列的行数量
    # actual3 = query("select * from root.tests.g5.d12")
    # # 判断是否符合预期
    # assert expect3 == actual3


# 测试带有类型推断的写入：往一个设备中写入多条记录
@pytest.mark.usefixtures('fixture_')
def test_insert_string_records_of_one_device():
    global session
    expect1 = 1
    session.insert_string_records_of_one_device("root.tests.g5.d20", [0],
                                                [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                  "BLOB", "STRING"]],
                                                [["true", "11", "11", "11.11", "10011.1", "test01", "11", "1970-01-01",
                                                  'b"x12x34"', "string01"]])
    # 计算实际时间序列的行数量
    actual1 = query("select * from root.tests.g5.d20")
    # 判断是否符合预期
    assert expect1 == actual1

    # TODO:未支持处理空值
    # expect2 = 1
    # session.insert_string_records_of_one_device("root.tests.g5.d20", [0],
    #                                             [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
    #                                               "BLOB", "STRING"]],
    #                                             [["true", None, "11", "11.11", "10011.1", "test01", "11", "1970-01-01",
    #                                               'b"x12x34"', "string01"]])
    # # 计算实际时间序列的行数量
    # actual2 = query("select * from root.tests.g5.d20")
    # # 判断是否符合预期
    # assert expect2 == actual2


# 测试带有类型推断的写入：往一个设备中写入多条对齐记录
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_string_records_of_one_device():
    global session
    expect1 = 1
    session.insert_aligned_string_records_of_one_device("root.tests.g5.d30", [0],
                                                        [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS",
                                                          "DATE",
                                                          "BLOB", "STRING"]],
                                                        [["true", "10", "11", "11.11", "10011.1", "test01", "11",
                                                          "1970-01-01",
                                                          'b"x12x34"', "string01"]])
    # 计算实际时间序列的行数量
    actual1 = query("select * from root.tests.g5.d30")
    # 判断是否符合预期
    assert expect1 == actual1

    # TODO:未支持处理空值
    # expect2 = 1
    # session.insert_aligned_string_records_of_one_device("root.tests.g5.d30", [0],
    #                                                     [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS",
    #                                                       "DATE",
    #                                                       "BLOB", "STRING"]],
    #                                                     [["true", None, "11", "11.11", "10011.1", "test01", "11",
    #                                                       "1970-01-01",
    #                                                       'b"x12x34"', "string01"]])
    # # 计算实际时间序列的行数量
    # actual2 = query("select * from root.tests.g5.d30")
    # # 判断是否符合预期
    # assert expect2 == actual2


# 测试直接写入
@pytest.mark.usefixtures('fixture_')
def test_insert_direct():
    global session
    # 测试非对齐tablet写入
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.fd_a" + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                       TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
             "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
             11, date(1970, 1, 1),
             '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                 'utf-8'),
             "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
            [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
             'Hello, World!'.encode('utf-8'), "string01"],
            [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
            [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
            [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
        ]
        timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
        session.insert_tablet(tablet_)
    # 测试非对齐numpy tablet写入
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.fd_b" + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                       TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
        np_values_ = [
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
        np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
        np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
        session.insert_tablet(np_tablet_)
    # 测试对齐tablet写入
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.d_c" + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                       TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
             "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
             11, date(1970, 1, 1),
             '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                 'utf-8'),
             "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
            [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
             'Hello, World!'.encode('utf-8'), "string01"],
            [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
            [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
            [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
        ]
        timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
        session.insert_aligned_tablet(tablet_)
    # 测试对齐numpy tablet写入
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.d_d" + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                       TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
        np_values_ = [
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
        np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
        np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
        session.insert_aligned_tablet(np_tablet_)


# 测试往非对齐时间序列写入一条 tablet 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_tablet():
    global session
    # 1、普通 Tablet 插入
    expect = 10
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
    session.insert_tablet(tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    # 判断是否符合预期
    assert expect == actual

    # 2、Numpy Tablet（不含空值）
    expect = 10
    device_id = "root.tests.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    session.insert_tablet(np_tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d2")
    # 判断是否符合预期
    assert expect == actual

    # 3、Numpy Tablet（含空值）
    expect = 10
    device_id = "root.tests.g1.d3"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    for i in range(len(measurements_)):
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
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_, np_bitmaps_)
    session.insert_tablet(np_tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d3")
    # 判断是否符合预期
    assert expect == actual


# 测试往对齐时间序列写入一条 tablet 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_tablet():
    global session
    # 1、普通 Tablet
    expect = 10
    device_id = "root.tests.g3.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
    session.insert_aligned_tablet(tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    # 判断是否符合预期
    assert expect == actual

    # 2、Numpy Tablet
    expect = 10
    device_id = "root.tests.g3.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    session.insert_aligned_tablet(np_tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d2")
    # 判断是否符合预期
    assert expect == actual

    # 3、Numpy Tablet（含空值）
    expect = 10
    device_id = "root.tests.g3.d3"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    for i in range(len(measurements_)):
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
    np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_, np_bitmaps_)
    session.insert_aligned_tablet(np_tablet_)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d3")
    # 判断是否符合预期
    assert expect == actual


# 测试往非对齐时间序列写入多条 tablet 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_tablets():
    global session
    # 1、普通 Tablet
    expect = 40
    device1_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet1_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)
    device1_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    tablet2_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)

    device2_id = "root.tests.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet3_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    device2_id = "root.tests.g2.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet4_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    tablet_lst = [tablet1_, tablet2_, tablet3_, tablet4_]
    session.insert_tablets(tablet_lst)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    actual = actual + query("select * from root.tests.g1.d2")
    actual = actual + query("select * from root.tests.g2.d1")
    # 判断是否符合预期
    assert expect == actual

    # 2、Numpy Tablet
    expect = 40
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet1_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([11, 12, 13, 14, 15, 16, 17, 18, 19, 20], TSDataType.INT64.np_dtype())
    np_tablet2_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.tests.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet3_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.tests.g2.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet4_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    np_tablet_list = [np_tablet1_, np_tablet2_, np_tablet3_, np_tablet4_]
    session.insert_tablets(np_tablet_list)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    actual = actual + query("select * from root.tests.g1.d2")
    actual = actual + query("select * from root.tests.g2.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试往对齐时间序列写入多条 tablet 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_tablets():
    global session
    # 1、普通 Tablet
    expect = 40
    device1_id = "root.tests.g3.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet1_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)
    device1_id = "root.tests.g3.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    tablet2_ = Tablet(device1_id, measurements_, data_types, values_, timestamps_)

    tablet_lst = [tablet1_, tablet2_]
    session.insert_aligned_tablets(tablet_lst)

    device2_id = "root.tests.g3.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet3_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    device2_id = "root.tests.g4.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                  TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet4_ = Tablet(device2_id, measurements_, data_types, values_, timestamps_)

    tablet_lst = [tablet1_, tablet2_, tablet3_, tablet4_]
    session.insert_aligned_tablets(tablet_lst)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    actual = actual + query("select * from root.tests.g3.d2")
    actual = actual + query("select * from root.tests.g4.d1")
    # 判断是否符合预期
    assert expect == actual

    # 2、Numpy Tablet
    expect = 40
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet1_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([11, 12, 13, 14, 15, 16, 17, 18, 19, 20], TSDataType.INT64.np_dtype())
    np_tablet2_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.tests.g1.d2"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet3_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    device_id = "root.tests.g2.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
    np_values_ = [
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
    np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
    np_tablet4_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)

    np_tablet_list = [np_tablet1_, np_tablet2_, np_tablet3_, np_tablet4_]
    session.insert_aligned_tablets(np_tablet_list)
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    actual = actual + query("select * from root.tests.g3.d2")
    actual = actual + query("select * from root.tests.g4.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试往非对齐时间序列写入一条 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_record():
    global session
    expect = 10
    session.insert_record("root.tests.g1.d1", 1,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'),
                           "1234567890"])
    session.insert_record("root.tests.g1.d1", 2,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567,
                           "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1),
                           'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"])
    session.insert_record("root.tests.g1.d1", 3,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567,
                           "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31),
                           '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
    session.insert_record("root.tests.g1.d1", 4,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"])
    session.insert_record("root.tests.g1.d1", 5,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11,
                           date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'),
                           "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
    session.insert_record("root.tests.g1.d1", 6,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, 10, 11, 4.123456, 4.123456789012345,
                           "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                           11, date(1970, 1, 1),
                           '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                               'utf-8'),
                           "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
    session.insert_record("root.tests.g1.d1", 7,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
                           'Hello, World!'.encode('utf-8'), "string01"])
    session.insert_record("root.tests.g1.d1", 8,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""])
    session.insert_record("root.tests.g1.d1", 9,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'),
                           "string01"])
    session.insert_record("root.tests.g1.d1", 10,
                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                           TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                          [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'),
                           "string01"])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试往对齐时间序列写入一条 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_record():
    global session
    expect = 9
    session.insert_aligned_record("root.tests.g3.d1", 1,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1),
                                   '1234567890'.encode('utf-8'), "1234567890"])
    session.insert_aligned_record("root.tests.g3.d1", 2,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567,
                                   "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1),
                                   'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"])
    session.insert_aligned_record("root.tests.g3.d1", 3,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567,
                                   "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31),
                                   '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
    session.insert_aligned_record("root.tests.g3.d1", 4,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'),
                                   "没问题"])
    session.insert_aligned_record("root.tests.g3.d1", 5,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11,
                                   date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'),
                                   "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
    session.insert_aligned_record("root.tests.g3.d1", 6,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, 10, 11, 4.123456, 4.123456789012345,
                                   "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                                   11, date(1970, 1, 1),
                                   '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                                       'utf-8'),
                                   "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
    session.insert_aligned_record("root.tests.g3.d1", 7,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
                                   'Hello, World!'.encode('utf-8'), "string01"])
    # session.insert_aligned_record("root.tests.g1.d1", 8,
    #                       ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
    #                       [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
    #                       [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""])
    session.insert_aligned_record("root.tests.g3.d1", 9,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1),
                                   'Hello, World!'.encode('utf-8'), "string01"])
    session.insert_aligned_record("root.tests.g3.d1", 10,
                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                   "STRING"],
                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                   TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                   TSDataType.BLOB, TSDataType.STRING],
                                  [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1),
                                   'Hello, World!'.encode('utf-8'), "string01"])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试往非对齐时间序列写入多条 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_records():
    global session
    expect = 9
    session.insert_records(
        ["root.tests.g1.d1", "root.tests.g1.d2", "root.tests.g2.d1", "root.tests.g1.d1", "root.tests.g1.d1",
         "root.tests.g1.d1", "root.tests.g1.d1", "root.tests.g1.d1", "root.tests.g1.d1"],
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
        [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]],
        [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]],
        [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
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
         [None, None, None, None, None, "None", None, date(1970, 1, 1), 'None'.encode('utf-8'), "None"],
         [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
         [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    actual = actual + query("select * from root.tests.g1.d2")
    actual = actual + query("select * from root.tests.g2.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试往对齐时间序列写入多条 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_records():
    global session
    expect = 9
    session.insert_aligned_records(
        ["root.tests.g3.d1", "root.tests.g3.d2", "root.tests.g4.d1", "root.tests.g3.d1", "root.tests.g3.d1",
         "root.tests.g3.d1", "root.tests.g3.d1", "root.tests.g3.d1", "root.tests.g3.d1"],
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
        [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
         ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]],
        [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
         [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
          TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]],
        [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
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
         [None, None, None, None, None, "None", None, date(1970, 1, 1), 'None'.encode('utf-8'), "None"],
         [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
         [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    actual = actual + query("select * from root.tests.g3.d2")
    actual = actual + query("select * from root.tests.g4.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试插入同属于一个非对齐 device 的多个 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_records_of_one_device():
    global session
    expect = 9
    session.insert_records_of_one_device("root.tests.g1.d1",
                                         [1, 2, 3, 4, 5, 6, 7, 8, 9],
                                         [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          # ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"],
                                          ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB",
                                           "STRING"]],
                                         [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          # [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING],
                                          [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
                                           TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE,
                                           TSDataType.BLOB, TSDataType.STRING]],
                                         [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1),
                                           '1234567890'.encode('utf-8'), "1234567890"],
                                          [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567,
                                           "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1),
                                           'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
                                          [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567,
                                           "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31),
                                           '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
                                           "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
                                          [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1),
                                           '没问题'.encode('utf-8'), "没问题"],
                                          [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/",
                                           11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'),
                                           "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
                                          [True, 10, 11, 4.123456, 4.123456789012345,
                                           "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                                           11, date(1970, 1, 1),
                                           '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                                               'utf-8'),
                                           "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
                                          [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
                                           'Hello, World!'.encode('utf-8'), "string01"],
                                          # [None, None, None, None, None, "None", None, date(1970, 1, 1), 'None'.encode('utf-8'), "None"],
                                          [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1),
                                           'Hello, World!'.encode('utf-8'), "string01"],
                                          [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1),
                                           'Hello, World!'.encode('utf-8'), "string01"]])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g1.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试插入同属于一个对齐 device 的多个 Record 数据
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_records_of_one_device():
    global session
    expect = 9
    session.insert_aligned_records_of_one_device("root.tests.g3.d1",
                                                 [1, 2, 3, 4, 5, 6, 7, 8, 9],
                                                 [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  # ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"],
                                                  ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE",
                                                   "BLOB", "STRING"]],
                                                 [[TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  # [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING],
                                                  [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64,
                                                   TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
                                                   TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB,
                                                   TSDataType.STRING]],
                                                 [[False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1),
                                                   '1234567890'.encode('utf-8'), "1234567890"],
                                                  [True, -2147483648, -9223372036854775808, -0.12345678,
                                                   -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz",
                                                   -9223372036854775808, date(1000, 1, 1),
                                                   'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'),
                                                   "abcdefghijklmnopqrstuvwsyz"],
                                                  [True, 2147483647, 9223372036854775807, 0.123456789,
                                                   0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~",
                                                   9223372036854775807, date(9999, 12, 31),
                                                   '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'),
                                                   "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
                                                  [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1),
                                                   '没问题'.encode('utf-8'), "没问题"],
                                                  [True, -1, -1, 1.1234567, 1.1234567890123456,
                                                   "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1),
                                                   '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'),
                                                   "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
                                                  [True, 10, 11, 4.123456, 4.123456789012345,
                                                   "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
                                                   11, date(1970, 1, 1),
                                                   '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                                                       'utf-8'),
                                                   "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
                                                  [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11,
                                                   date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
                                                  # [None, None, None, None, None, "None", None, date(1970, 1, 1), 'None'.encode('utf-8'), "None"],
                                                  [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1),
                                                   'Hello, World!'.encode('utf-8'), "string01"],
                                                  [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1),
                                                   'Hello, World!'.encode('utf-8'), "string01"]])
    # 计算实际时间序列的行数量
    actual = query("select * from root.tests.g3.d1")
    # 判断是否符合预期
    assert expect == actual


# 测试直接写入
@pytest.mark.usefixtures('fixture_')
def test_insert_direct():
    global session
    # 测试非对齐tablet写入
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.fd_a" + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                       TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
             "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
             11, date(1970, 1, 1),
             '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                 'utf-8'),
             "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
            [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
             'Hello, World!'.encode('utf-8'), "string01"],
            [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
            [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
            [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
        ]
        timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
        session.insert_tablet(tablet_)
    # 测试非对齐numpy tablet写入
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.fd_b" + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                       TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
        np_values_ = [
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
        np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
        np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
        session.insert_tablet(np_tablet_)
    # 测试对齐tablet写入
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.d_c" + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                       TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
             "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题",
             11, date(1970, 1, 1),
             '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode(
                 'utf-8'),
             "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
            [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1),
             'Hello, World!'.encode('utf-8'), "string01"],
            [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
            [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
            [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
        ]
        timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
        session.insert_aligned_tablet(tablet_)
    # 测试对齐numpy tablet写入
    num = 0
    for i in range(1, 10):
        device_id = "root.repeat.g1.d_d" + str(num)
        num = num + 1
        measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
        data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                       TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
        np_values_ = [
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
        np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
        np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
        session.insert_aligned_tablet(np_tablet_)


# 测试删除多个时间序列全部的数据
@pytest.mark.usefixtures('fixture_')
def test_delete_data1():
    # 写入数据
    expect = 10
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
    session.insert_tablet(tablet_)
    # 计算实际时间序列的行数量
    actual1 = query("select * from " + device_id)
    # 判断是否符合预期
    assert expect == actual1
    # 删除全部数据
    path_list = []
    for i in range(len(measurements_)):
        path_list.append(device_id + "." + measurements_[i])
    session.delete_data(path_list, 9223372036854775807)
    # 计算实际时间序列的行数量
    actual2 = query("select * from " + device_id)
    # 判断是否符合预期
    assert 0 == actual2


# 测试删除多个时间序列指定范围的数据（删除全部数据）
@pytest.mark.usefixtures('fixture_')
def test_delete_data2():
    # 写入数据
    expect = 10
    device_id = "root.tests.g1.d1"
    measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
    data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                   TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
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
    timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
    session.insert_tablet(tablet_)
    # 计算实际时间序列的行数量
    actual1 = query("select * from " + device_id)
    # 判断是否符合预期
    assert expect == actual1
    # 删除全部数据
    path_list = []
    for i in range(len(measurements_)):
        path_list.append(device_id + "." + measurements_[i])
    session.delete_data_in_range(path_list, -9223372036854775808, 9223372036854775807)
    # 计算实际时间序列的行数量
    actual2 = query("select * from " + device_id)
    # 判断是否符合预期
    assert 0 == actual2


# 测试检查顺序函数：check_sorted
@pytest.mark.usefixtures('fixture_')
def test_check_sorted():
    global session
    timestamps1_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert True == session.check_sorted(
        timestamps1_), "The expected result is inconsistent with the actual result, expect: True, actual: " + str(
        session.check_sorted(timestamps1_))
    timestamps2_ = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    assert False == session.check_sorted(
        timestamps2_), "The expected result is inconsistent with the actual result, expect: False, actual: " + str(
        session.check_sorted(timestamps2_))


#############异常情况#############
# 1、测试带有类型推断的写入：往一个设备中写入多条记录
@pytest.mark.usefixtures('fixture_')
def test_insert_string_records_of_one_device_error():
    global session
    try:
        session.insert_string_records_of_one_device("root.tests.g5.d1", [0, 1],
                                                    [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS",
                                                      "DATE",
                                                      "BLOB", "STRING"]],
                                                    [["true", "10", "11", "11.11", "10011.1", "test01", "11",
                                                      "1970-01-01",
                                                      'b"x12x34"', "string01"]])
    except Exception as e:
        assert isinstance(e, RuntimeError) and str(
            e) == "insert records of one device error: times, measurementsList and valuesList's size should be equal!", "期待报错信息与实际不一致，期待：RuntimeError: insert records of one device error: times, measurementsList and valuesList's size should be equal!，实际：" + type(
            e).__name__ + ":" + str(e)


# 2、测试带有类型推断的写入：往一个设备中写入多条对齐记录
@pytest.mark.usefixtures('fixture_')
def test_insert_aligned_string_records_of_one_device_error():
    global session
    try:
        session.insert_aligned_string_records_of_one_device("root.tests.g5.d10", [0, 1],
                                                            [["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT",
                                                              "TS",
                                                              "DATE",
                                                              "BLOB", "STRING"]],
                                                            [["true", "10", "11", "11.11", "10011.1", "test01", "11",
                                                              "1970-01-01",
                                                              'b"x12x34"', "string01"]])
    except Exception as e:
        assert isinstance(e, RuntimeError) and str(
            e) == "insert records of one device error: times, measurementsList and valuesList's size should be equal!", "期待报错信息与实际不一致，期待：RuntimeError: insert records of one device error: times, measurementsList and valuesList's size should be equal!，实际：" + type(
            e).__name__ + ":" + str(e)


# 3、测试insert_records数量不一致情况
@pytest.mark.usefixtures('fixture_')
def test_insert_records_error():
    global session
    try:
        session.insert_records(
            ["root.tests_insert_error3.d1"],
            [1, 2],
            [["s1", "s2"], ["s1", "s2"]],
            [[TSDataType.BOOLEAN, TSDataType.INT32], [TSDataType.BOOLEAN, TSDataType.INT32]],
            [[False, 0], [True, 1]])
    except Exception as e:
        assert isinstance(e, RuntimeError) and str(
            e) == "insert records of one device error: times, measurementsList and valuesList's size should be equal!", "期待报错信息与实际不一致，期待：RuntimeError: insert records of one device error: times, measurementsList and valuesList's size should be equal!，实际：" + type(
            e).__name__ + ":" + str(e)


# 4、测试 insert_str_record 数量不一致情况
@pytest.mark.usefixtures('fixture_')
def test_insert_str_record_error():
    global session
    try:
        session.insert_str_record(
            "root.tests_insert_error4.d1",
            1,
            ["s1"],
            ["False", "0"])
    except Exception as e:
        assert isinstance(e, RuntimeError) and str(
            e) == "length of measurements does not equal to length of values!", "期待报错信息与实际不一致，期待：RuntimeError: length of measurements does not equal to length of values!，实际：" + type(
            e).__name__ + ":" + str(e)


# 5、测试 insert_record 数量不一致情况
@pytest.mark.usefixtures('fixture_')
def test_insert_record_error():
    global session
    try:
        session.insert_record(
            "root.tests_insert_error5.d1",
            1,
            ["s1"],
            [TSDataType.BOOLEAN],
            ["False", "0"])
    except Exception as e:
        assert isinstance(e, RuntimeError) and str(
            e) == "length of data types does not equal to length of values!", "期待报错信息与实际不一致，期待：RuntimeError: length of data types does not equal to length of values!，实际：" + type(
            e).__name__ + ":" + str(e)


