# import numpy as np
# import pytest
# import yaml
# import os
# from datetime import date
# from iotdb.Session import Session
# from iotdb.SessionPool import SessionPool, PoolConfig
# from iotdb.utils.BitMap import BitMap
# from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
# from iotdb.utils.Tablet import Tablet
# from iotdb.utils.NumpyTablet import NumpyTablet
# from datetime import date
#
# """
#  Title：测试树模型python查询接口—正常情况
#  Describe：基于1.3.4.1版本树模型
#  Author：肖林捷
#  Date：2024/12/10
# """
#
# session = None
# session_pool= None
#
# def get_session_():
#     global session
#     global session_pool
#     session = Session( "172.20.31.63", "6667", "root", "root")
#     session.open()
#
#     # pool_config = PoolConfig("172.20.31.63",6667, "root", "root")
#     # session_pool = SessionPool(pool_config, 10, 0)
#     # session = session_pool.get_session()
#
#     return session
#
# def create_database(session):
#     # 创建数据库
#     session.set_storage_group("root.tests.g1")
#     session.set_storage_group("root.tests.g2")
#     session.set_storage_group("root.tests.g3")
#     session.set_storage_group("root.tests.g4")
#     session.set_storage_group("root.tests.g5")
#     session.set_storage_group("root.tests.g6")
#
# def create_timeseries(session):
#     # 1、创建单个时间序列
#     session.create_time_series("root.tests.g5.d1.STRING1", TSDataType.STRING, TSEncoding.DICTIONARY, Compressor.UNCOMPRESSED,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.STRING2", TSDataType.STRING, TSEncoding.PLAIN, Compressor.LZ4,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.STRING3", TSDataType.STRING, TSEncoding.DICTIONARY, Compressor.GZIP,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.STRING4", TSDataType.STRING, TSEncoding.PLAIN, Compressor.ZSTD,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.STRING5", TSDataType.STRING, TSEncoding.DICTIONARY, Compressor.LZMA2,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.STRING6", TSDataType.STRING, TSEncoding.PLAIN, Compressor.LZ4,
#         props=None, tags=None, attributes=None, alias=None)
#
#     session.create_time_series("root.tests.g5.d1.TS1", TSDataType.TIMESTAMP, TSEncoding.PLAIN, Compressor.GZIP,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.TS2", TSDataType.TIMESTAMP, TSEncoding.RLE, Compressor.UNCOMPRESSED,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.TS3", TSDataType.TIMESTAMP, TSEncoding.TS_2DIFF, Compressor.LZ4,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.TS4", TSDataType.TIMESTAMP, TSEncoding.ZIGZAG, Compressor.ZSTD,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.TS5", TSDataType.TIMESTAMP, TSEncoding.CHIMP, Compressor.LZMA2,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.TS6", TSDataType.TIMESTAMP, TSEncoding.SPRINTZ, Compressor.GZIP,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.TS7", TSDataType.TIMESTAMP, TSEncoding.RLBE, Compressor.LZ4,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.TS8", TSDataType.TIMESTAMP, TSEncoding.GORILLA, Compressor.GZIP,
#         props=None, tags=None, attributes=None, alias=None)
#
#     session.create_time_series("root.tests.g5.d1.DATE1", TSDataType.DATE, TSEncoding.PLAIN, Compressor.GZIP,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.DATE2", TSDataType.DATE, TSEncoding.RLE, Compressor.UNCOMPRESSED,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.DATE3", TSDataType.DATE, TSEncoding.TS_2DIFF, Compressor.LZ4,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.DATE4", TSDataType.DATE, TSEncoding.ZIGZAG, Compressor.ZSTD,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.DATE5", TSDataType.DATE, TSEncoding.CHIMP, Compressor.LZMA2,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.DATE6", TSDataType.DATE, TSEncoding.SPRINTZ, Compressor.GZIP,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.DATE7", TSDataType.DATE, TSEncoding.RLBE, Compressor.LZ4,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.DATE8", TSDataType.DATE, TSEncoding.GORILLA, Compressor.GZIP,
#         props=None, tags=None, attributes=None, alias=None)
#
#     session.create_time_series("root.tests.g5.d1.BLOB1", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.UNCOMPRESSED,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.BLOB2", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.LZ4,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.BLOB3", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.GZIP,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.BLOB4", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.ZSTD,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.BLOB5", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.LZMA2,
#         props=None, tags=None, attributes=None, alias=None)
#     session.create_time_series("root.tests.g5.d1.BLOB6", TSDataType.BLOB, TSEncoding.PLAIN, Compressor.LZ4,
#         props=None, tags=None, attributes=None, alias=None)
#
#
#     # 2、创建多个
#     ts_path_lst = ["root.tests.g1.d1.BOOLEAN", "root.tests.g1.d1.INT32", "root.tests.g1.d1.INT64", "root.tests.g1.d1.FLOAT", "root.tests.g1.d1.DOUBLE", "root.tests.g1.d1.TEXT", "root.tests.g1.d1.TS", "root.tests.g1.d1.DATE", "root.tests.g1.d1.BLOB", "root.tests.g1.d1.STRING",
#                    "root.tests.g1.d2.BOOLEAN", "root.tests.g1.d2.INT32", "root.tests.g1.d2.INT64", "root.tests.g1.d2.FLOAT", "root.tests.g1.d2.DOUBLE", "root.tests.g1.d2.TEXT", "root.tests.g1.d2.TS", "root.tests.g1.d2.DATE", "root.tests.g1.d2.BLOB", "root.tests.g1.d2.STRING",
#                    "root.tests.g1.d3.BOOLEAN", "root.tests.g1.d3.INT32", "root.tests.g1.d3.INT64", "root.tests.g1.d3.FLOAT", "root.tests.g1.d3.DOUBLE", "root.tests.g1.d3.TEXT", "root.tests.g1.d3.TS", "root.tests.g1.d3.DATE", "root.tests.g1.d3.BLOB", "root.tests.g1.d3.STRING",
#                    "root.tests.g1.d4.BOOLEAN", "root.tests.g1.d4.INT32", "root.tests.g1.d4.INT64", "root.tests.g1.d4.FLOAT", "root.tests.g1.d4.DOUBLE", "root.tests.g1.d4.TEXT", "root.tests.g1.d4.TS", "root.tests.g1.d4.DATE", "root.tests.g1.d4.BLOB", "root.tests.g1.d4.STRING",
#                    "root.tests.g2.d1.BOOLEAN", "root.tests.g2.d1.INT32", "root.tests.g2.d1.INT64", "root.tests.g2.d1.FLOAT", "root.tests.g2.d1.DOUBLE", "root.tests.g2.d1.TEXT", "root.tests.g2.d1.TS", "root.tests.g2.d1.DATE", "root.tests.g2.d1.BLOB", "root.tests.g2.d1.STRING"]
#     data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,
#                      TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,
#                      TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,
#                      TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,
#                      TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
#     encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,
#                     TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,
#                     TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,
#                     TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,
#                     TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY]
#     compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,
#                       Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,
#                       Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,
#                       Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,
#                       Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP]
#     session.create_multi_time_series(
#         ts_path_lst, data_type_lst, encoding_lst, compressor_lst,
#         props_lst=None, tags_lst=None, attributes_lst=None, alias_lst=None
#     )
#
# def create_aligned_timeseries(session):
#     device_id = "root.tests.g3.d1"
#     measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING",]
#     data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,]
#     encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,]
#     compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,]
#     session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)
#
#     device_id = "root.tests.g3.d2"
#     measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING",]
#     data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,]
#     encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,]
#     compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,]
#     session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)
#
#     device_id = "root.tests.g3.d3"
#     measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING",]
#     data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,]
#     encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,]
#     compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,]
#     session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)
#
#     device_id = "root.tests.g3.d4"
#     measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING",]
#     data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,]
#     encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,]
#     compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,]
#     session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)
#
#     device_id = "root.tests.g4.d1"
#     measurements_lst = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING",]
#     data_type_lst = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING,]
#     encoding_lst = [TSEncoding.RLE, TSEncoding.CHIMP, TSEncoding.ZIGZAG, TSEncoding.RLBE, TSEncoding.SPRINTZ, TSEncoding.DICTIONARY, TSEncoding.TS_2DIFF, TSEncoding.CHIMP, TSEncoding.PLAIN, TSEncoding.DICTIONARY,]
#     compressor_lst = [Compressor.UNCOMPRESSED, Compressor.SNAPPY, Compressor.LZ4, Compressor.GZIP, Compressor.ZSTD, Compressor.LZMA2, Compressor.GZIP, Compressor.GZIP, Compressor.GZIP,  Compressor.GZIP,]
#     session.create_aligned_time_series(device_id, measurements_lst, data_type_lst, encoding_lst, compressor_lst)
#
# def insert_tablet():
#     global session
#     # 1、普通 Tablet 插入
#     device_id = "root.tests.g1.d1"
#     measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
#     data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
#     values_ = [
#             [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
#             [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
#             [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
#             [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
#             [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
#             [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
#             [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
#             [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
#             [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
#             [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
#         ]
#     timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
#     tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
#     session.insert_tablet(tablet_)
#
#     # 2、Numpy Tablet（不含空值）
#     device_id = "root.tests.g1.d2"
#     measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
#     data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
#     np_values_ = [
#         np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
#         np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
#         np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
#         np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
#         np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
#         np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
#         np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
#         np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
#         np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
#         np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
#     ]
#     np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
#     np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
#     session.insert_tablet(np_tablet_)
#
#     # 3、Numpy Tablet（含空值）
#     device_id = "root.tests.g1.d3"
#     measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
#     data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
#     np_values_ = [
#         np.array([False, True, False, True, False, True, False, True, False, False], TSDataType.BOOLEAN.np_dtype()),
#         np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
#         np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
#         np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
#         np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
#         np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
#         np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
#         np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
#         np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
#         np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
#     ]
#     np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
#     np_bitmaps_ = []
#     for i in range(len(measurements_)):
#         np_bitmaps_.append(BitMap(len(np_timestamps_)))
#     np_bitmaps_[0].mark(9)
#     np_bitmaps_[1].mark(8)
#     np_bitmaps_[2].mark(9)
#     np_bitmaps_[4].mark(8)
#     np_bitmaps_[5].mark(9)
#     np_bitmaps_[6].mark(8)
#     np_bitmaps_[7].mark(9)
#     np_bitmaps_[8].mark(8)
#     np_bitmaps_[9].mark(9)
#     np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_, np_bitmaps_)
#     session.insert_tablet(np_tablet_)
#
#     # 4、Numpy Tablet（全为空值）
#     device_id = "root.tests.g1.d4"
#     measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
#     data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
#     np_values_ = [
#         np.array([False], TSDataType.BOOLEAN.np_dtype()),
#         np.array([0], TSDataType.INT32.np_dtype()),
#         np.array([0], TSDataType.INT64.np_dtype()),
#         np.array([1.1], TSDataType.FLOAT.np_dtype()),
#         np.array([0.0], TSDataType.DOUBLE.np_dtype()),
#         np.array(["1234567890"], TSDataType.TEXT.np_dtype()),
#         np.array([0], TSDataType.TIMESTAMP.np_dtype()),
#         np.array([date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
#         np.array(['1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
#         np.array(["1234567890"], TSDataType.STRING.np_dtype())
#     ]
#     np_timestamps_ = np.array([1], TSDataType.INT64.np_dtype())
#     np_bitmaps_ = []
#     for i in range(len(measurements_)):
#         np_bitmaps_.append(BitMap(len(np_timestamps_)))
#     np_bitmaps_[0].mark(0)
#     np_bitmaps_[1].mark(0)
#     np_bitmaps_[2].mark(0)
#     np_bitmaps_[3].mark(0)
#     np_bitmaps_[4].mark(0)
#     np_bitmaps_[5].mark(0)
#     np_bitmaps_[6].mark(0)
#     np_bitmaps_[7].mark(0)
#     np_bitmaps_[8].mark(0)
#     np_bitmaps_[9].mark(0)
#     np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_, np_bitmaps_)
#     session.insert_tablet(np_tablet_)
#
# def insert_aligned_tablet():
#     global session
#     # 1、普通 Tablet
#     device_id = "root.tests.g3.d1"
#     measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
#     data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
#     values_ = [
#             [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"],
#             [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"],
#             [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"],
#             [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"],
#             [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"],
#             [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"],
#             [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"],
#             [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""],
#             [True, -0, -0, -0.0, -0.0, "    ", 11, date(1970, 1, 1), '    '.encode('utf-8'), "    "],
#             [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"]
#     ]
#     timestamps_ = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
#     tablet_ = Tablet(device_id, measurements_, data_types_, values_, timestamps_)
#     session.insert_aligned_tablet(tablet_)
#
#     # 2、Numpy Tablet
#     device_id = "root.tests.g3.d2"
#     measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
#     data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
#     np_values_ = [
#         np.array([False, True, False, True, False, True, False, True, False, True], TSDataType.BOOLEAN.np_dtype()),
#         np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
#         np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
#         np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
#         np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
#         np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
#         np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
#         np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
#         np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
#         np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
#     ]
#     np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
#     np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_)
#     session.insert_aligned_tablet(np_tablet_)
#
#     # 3、Numpy Tablet（含空值）
#     device_id = "root.tests.g3.d3"
#     measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
#     data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
#     np_values_ = [
#         np.array([False, True, False, True, False, True, False, True, False, False], TSDataType.BOOLEAN.np_dtype()),
#         np.array([0, -2147483648, 2147483647, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT32.np_dtype()),
#         np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.INT64.np_dtype()),
#         np.array([0.0, -0.12345678, 0.123456789, 1.0, 1.1234567, 4.123456, 12.12345, -0.0, 1.1, 1.1], TSDataType.FLOAT.np_dtype()),
#         np.array([0.0, -0.12345678901234567, 0.12345678901234567, 1.0, 1.1234567890123456, 4.123456789012345, 12.12345678901234, -0.0, 1.1, 1.1], TSDataType.DOUBLE.np_dtype()),
#         np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.TEXT.np_dtype()),
#         np.array([0, -9223372036854775808, 9223372036854775807, 1, -1, 10, -10, -0, 10, 10], TSDataType.TIMESTAMP.np_dtype()),
#         np.array([date(1970, 1, 1), date(1000, 1, 1), date(9999, 12, 31), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
#         np.array(['1234567890'.encode('utf-8'), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), '没问题'.encode('utf-8'), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), ''.encode('utf-8'), '   '.encode('utf-8'), '1234567890'.encode('utf-8'), '1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
#         np.array(["1234567890", "abcdefghijklmnopqrstuvwsyz", "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", "没问题", "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", "", "    ", "test01", "test01"], TSDataType.STRING.np_dtype())
#     ]
#     np_timestamps_ = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], TSDataType.INT64.np_dtype())
#     np_bitmaps_ = []
#     for i in range(len(measurements_)):
#         np_bitmaps_.append(BitMap(len(np_timestamps_)))
#     np_bitmaps_[0].mark(9)
#     np_bitmaps_[1].mark(8)
#     np_bitmaps_[2].mark(9)
#     np_bitmaps_[4].mark(8)
#     np_bitmaps_[5].mark(9)
#     np_bitmaps_[6].mark(8)
#     np_bitmaps_[7].mark(9)
#     np_bitmaps_[8].mark(8)
#     np_bitmaps_[9].mark(9)
#     np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_, np_bitmaps_)
#     session.insert_aligned_tablet(np_tablet_)
#
#     # 4、Numpy Tablet（全空值）
#     device_id = "root.tests.g3.d4"
#     measurements_ = ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"]
#     data_types_ = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING]
#     np_values_ = [
#         np.array([False], TSDataType.BOOLEAN.np_dtype()),
#         np.array([0], TSDataType.INT32.np_dtype()),
#         np.array([0], TSDataType.INT64.np_dtype()),
#         np.array([0.0], TSDataType.FLOAT.np_dtype()),
#         np.array([0.0], TSDataType.DOUBLE.np_dtype()),
#         np.array(["1234567890"], TSDataType.TEXT.np_dtype()),
#         np.array([0], TSDataType.TIMESTAMP.np_dtype()),
#         np.array([date(1970, 1, 1)], TSDataType.DATE.np_dtype()),
#         np.array(['1234567890'.encode('utf-8')], TSDataType.BLOB.np_dtype()),
#         np.array(["1234567890"], TSDataType.STRING.np_dtype())
#     ]
#     np_timestamps_ = np.array([1], TSDataType.INT64.np_dtype())
#     np_bitmaps_ = []
#     for i in range(len(measurements_)):
#         np_bitmaps_.append(BitMap(len(np_timestamps_)))
#     np_bitmaps_[0].mark(0)
#     np_bitmaps_[1].mark(0)
#     np_bitmaps_[2].mark(0)
#     np_bitmaps_[3].mark(0)
#     np_bitmaps_[4].mark(0)
#     np_bitmaps_[5].mark(0)
#     np_bitmaps_[6].mark(0)
#     np_bitmaps_[7].mark(0)
#     np_bitmaps_[8].mark(0)
#     np_bitmaps_[9].mark(0)
#     np_tablet_ = NumpyTablet(device_id, measurements_, data_types_, np_values_, np_timestamps_, np_bitmaps_)
#     session.insert_aligned_tablet(np_tablet_)
#
# def insert_record():
#     global session
#     session.insert_record("root.tests.g1.d1", 1,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"])
#     session.insert_record("root.tests.g1.d1", 2,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"])
#     session.insert_record("root.tests.g1.d1", 3,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
#     session.insert_record("root.tests.g1.d1", 4,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"])
#     session.insert_record("root.tests.g1.d1", 5,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
#     session.insert_record("root.tests.g1.d1", 6,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
#     session.insert_record("root.tests.g1.d1", 7,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
#     # session.insert_record("root.tests.g1.d1", 8,
#     #                       ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#     #                       [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#     #                       [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""])
#     session.insert_record("root.tests.g1.d1", 9,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
#     session.insert_record("root.tests.g1.d1", 10,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
#
# def insert_aligned_record():
#     global session
#     session.insert_aligned_record("root.tests.g3.d1", 1,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [False, 0, 0, 0.0, 0.0, "1234567890", 0, date(1970, 1, 1), '1234567890'.encode('utf-8'), "1234567890"])
#     session.insert_aligned_record("root.tests.g3.d1", 2,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, -2147483648, -9223372036854775808, -0.12345678, -0.12345678901234567, "abcdefghijklmnopqrstuvwsyz", -9223372036854775808, date(1000, 1, 1), 'abcdefghijklmnopqrstuvwsyz'.encode('utf-8'), "abcdefghijklmnopqrstuvwsyz"])
#     session.insert_aligned_record("root.tests.g3.d1", 3,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, 2147483647, 9223372036854775807, 0.123456789, 0.12345678901234567, "!@#$%^&*()_+}{|:'`~-=[];,./<>?~", 9223372036854775807, date(9999, 12, 31), '!@#$%^&*()_+}{|:`~-=[];,./<>?~'.encode('utf-8'), "!@#$%^&*()_+}{|:`~-=[];,./<>?~"])
#     session.insert_aligned_record("root.tests.g3.d1", 4,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, 1, 1, 1.0, 1.0, "没问题", 1, date(1970, 1, 1), '没问题'.encode('utf-8'), "没问题"])
#     session.insert_aligned_record("root.tests.g3.d1", 5,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, -1, -1, 1.1234567, 1.1234567890123456, "！@#￥%……&*（）——|：“《》？·【】、；‘，。/", 11, date(1970, 1, 1), '！@#￥%……&*（）——|：“《》？·【】、；‘，。/'.encode('utf-8'), "！@#￥%……&*（）——|：“《》？·【】、；‘，。/"])
#     session.insert_aligned_record("root.tests.g3.d1", 6,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, 10, 11, 4.123456, 4.123456789012345, "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:'`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题", 11, date(1970, 1, 1), '1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题'.encode('utf-8'), "1234567890abcdefghijklmnopqrstuvwsyz!@#$%^&*()_+}{|:`~-=[];,./<>?~！@#￥%……&*（）——|：“《》？·【】、；‘，。/没问题"])
#     session.insert_aligned_record("root.tests.g3.d1", 7,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, -10, -11, 12.12345, 12.12345678901234, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
#     # session.insert_aligned_record("root.tests.g1.d1", 8,
#     #                       ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#     #                       [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#     #                       [None, None, None, None, None, "", None, date(1970, 1, 1), ''.encode('utf-8'), ""])
#     session.insert_aligned_record("root.tests.g3.d1", 9,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, -0, -0, -0.0, -0.0, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
#     session.insert_aligned_record("root.tests.g3.d1", 10,
#                           ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "TS", "DATE", "BLOB", "STRING"],
#                           [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT, TSDataType.TIMESTAMP, TSDataType.DATE, TSDataType.BLOB, TSDataType.STRING],
#                           [True, 10, 11, 1.1, 10011.1, "test01", 11, date(1970, 1, 1), 'Hello, World!'.encode('utf-8'), "string01"])
#
# def delete_database(session):
#     group_name_lst = ["root.tests.g1", "root.tests.g2", "root.tests.g3", "root.tests.g4", "root.tests.g5", "root.tests.g6"]
#     session.delete_storage_groups(group_name_lst)
#
# # 测试 execute_query_statement 函数
# def test_execute_query_statement():
#     # |----------查询数据库----------|
#     # 1、基本查询
#     with session.execute_query_statement(
#         "select last(*) from root.tests.ad_SameDevice_NoSameTimeStamp"
#     ) as session_data_set:
#         session_data_set.set_fetch_size(1024)
#         print(session_data_set.get_fetch_size())
#         print(session_data_set.get_column_names())
#         print(session_data_set.get_column_types())
#         while session_data_set.has_next():
#             print(session_data_set.next())
#
#     # # 2、SQL语句大小写
#     # with session.execute_query_statement(
#     #     "ShOw DaTaBaseS root.**"
#     # ) as session_data_set:
#     #     session_data_set.set_fetch_size(1024)
#     #     print(session_data_set.get_column_names())
#     #     print(session_data_set.get_column_types())
#     #     while session_data_set.has_next():
#     #         print(session_data_set.next())
#
#     # # 3、非法SQL查询
#     # with session.execute_query_statement(
#     #     "show database"
#     # ) as session_data_set:
#     #     session_data_set.set_fetch_size(1024)
#     #     print(session_data_set.get_column_names())
#     #     print(session_data_set.get_column_types())
#     #     while session_data_set.has_next():
#     #         print(session_data_set.next())
#
#     # # 4、设置超时时间
#     # with session.execute_query_statement(
#     #     "ShOw DaTaBaseS root.**", 1
#     # ) as session_data_set:
#     #     session_data_set.set_fetch_size(1024)
#     #     print(session_data_set.get_column_names())
#     #     print(session_data_set.get_column_types())
#     #     while session_data_set.has_next():
#     #         print(session_data_set.next())
#
#     # # |----------查询时间序列----------|
#     # # 1、基本查询
#     # with session.execute_query_statement(
#     #     "show timeseries "
#     # ) as session_data_set:
#     #     session_data_set.set_fetch_size(1024)
#     #     print(session_data_set.get_column_names())
#     #     print(session_data_set.get_column_types())
#     #     while session_data_set.has_next():
#     #         print(session_data_set.next())
#
#     # # 2、SQL语句大小写
#     # with session.execute_query_statement(
#     #     "ShOw TIMESERIES"
#     # ) as session_data_set:
#     #     session_data_set.set_fetch_size(1024)
#     #     print(session_data_set.get_column_names())
#     #     print(session_data_set.get_column_types())
#     #     while session_data_set.has_next():
#     #         print(session_data_set.next())
#
#     # # |----------查询设备----------|
#     # with session.execute_query_statement(
#     #     "ShOw devices"
#     # ) as session_data_set:
#     #     session_data_set.set_fetch_size(1024)
#     #     print(session_data_set.get_column_names())
#     #     print(session_data_set.get_column_types())
#     #     while session_data_set.has_next():
#     #         print(session_data_set.next())
#
#     # # |----------查询数据----------|
#     # with session.execute_query_statement(
#     #     "select * from root.tests.**"
#     # ) as session_data_set:
#     #     print(session_data_set.get_column_names())
#     #     print(session_data_set.get_column_types())
#     #     while session_data_set.has_next():
#     #         print(session_data_set.next())
#
# # 测试 execute_raw_data_query 函数
# def test_execute_raw_data_query():
#     # |----------查询数据库----------|
#     # 1、基本查询
#     # paths1 = ["root.tests.ad_NoSameDevice_NoSameTimeStamp_0_0.m_INT32_0_0", "root.tests.ad_NoSameDevice_SameTimeStamp_0_0.m_INT32_0_0", "root.tests.ad_SameDevice_NoSameTimeStamp.m_INT32_0_0", "root.tests.ad_SameDevice_SameTimeStamp.m_INT32_0_0","root.tests.d_NoSameDevice_NoSameTimeStamp_0_0.m_INT32_0_0", "root.tests.d_NoSameDevice_SameTimeStamp_0_0.m_INT32_0_0", "root.tests.d_SameDevice_NoSameTimeStamp.m_INT32_0_0", "root.tests.d_SameDevice_SameTimeStamp.m_INT32_0_0"]
#     paths1 = ["root.tests.ad_NoSameDevice_NoSameTimeStamp_0_0.m_INT32_0_0", "root.tests.ad_NoSameDevice_SameTimeStamp_0_0.m_INT32_0_0", "root.tests.ad_SameDevice_NoSameTimeStamp.m_INT32_0_0", "root.tests.ad_SameDevice_SameTimeStamp.m_INT32_0_0"]
#     with session.execute_raw_data_query(
#         paths1, -9223372036854775808, 9223372036854775807
#     ) as session_data_set:
#         print(session_data_set.get_column_names())
#         print(session_data_set.get_column_types())
#         while session_data_set.has_next():
#             print(session_data_set.next())
#
#     # 2、SQL语句大小写
#     # paths2 = ["root.tests.ad_NoSameDevice_NoSameTimeStamp_3_38.**"]
#     # with session.execute_raw_data_query(
#     #     paths2, -9223372036854775808, 9223372036854775807
#     # ) as session_data_set:
#     #     print(session_data_set.get_column_names())
#     #     print(session_data_set.get_column_types())
#     #     while session_data_set.has_next():
#     #         print(session_data_set.next())
#
#     # # 3、非法路径查询
#     # paths = ["Test@."]
#     # with session.execute_raw_data_query(
#     #     paths, 1, 1
#     # ) as session_data_set:
#     #     session_data_set.set_fetch_size(1024)
#     #     print(session_data_set.get_column_names())
#     #     print(session_data_set.get_column_types())
#     #     while session_data_set.has_next():
#     #         print(session_data_set.next())
#
# # 测试 execute_last_data_query 函数
# def test_execute_last_data_query():
#     # |----------查询数据库----------|
#     # 1、基本查询
#     paths = ["root.tests.ad_SameDevice_SameTimeStamp.m_INT32_0_0", "root.tests.ad_SameDevice_SameTimeStamp.m_BOOLEAN_0_0"]
#     with session.execute_last_data_query(
#         paths, 0
#     ) as session_data_set:
#         session_data_set.set_fetch_size(1024)
#         print(session_data_set.get_column_names())
#         print(session_data_set.get_column_types())
#         while session_data_set.has_next():
#             print(session_data_set.next())
#
#     # 2、SQL语句大小写
#     # paths = ["root.tests.ad_NoSameDevice_NoSameTimeStamp_3_38.**"]
#     # with session.execute_last_data_query(
#     #     paths, 0
#     # ) as session_data_set:
#     #     session_data_set.set_fetch_size(1024)
#     #     print(session_data_set.get_column_names())
#     #     print(session_data_set.get_column_types())
#     #     while session_data_set.has_next():
#     #         print(session_data_set.next())
#
#     # # 3、非法路径查询
#     # paths = ["Test@."]
#     # with session.execute_last_data_query(
#     #     paths, 1, 1
#     # ) as session_data_set:
#     #     session_data_set.set_fetch_size(1024)
#     #     print(session_data_set.get_column_names())
#     #     print(session_data_set.get_column_types())
#     #     while session_data_set.has_next():
#     #         print(session_data_set.next())
#
#
# # 获取session
# session = get_session_()
# # # 创建数据库
# create_database(session)
# # # 创建时间序列
# create_timeseries(session)
# create_aligned_timeseries(session)
# # # 写入数据
# insert_tablet()
# insert_aligned_tablet()
# insert_record()
# insert_aligned_record()
# # 查询
# test_execute_query_statement()
# test_execute_raw_data_query()
# test_execute_last_data_query()
#
# # 持续操作
# # for i in range(10000):
# #     try:
# #         print(f"当前迭代次数: {i + 1}")
# #         test_execute_last_data_query()
# #         test_execute_raw_data_query()
# #     except Exception as e:
# #         # 处理其他所有类型的异常
# #         print("捕获到其他异常：", e)
# #         print(f"当前迭代次数: {i + 1}")
# #         test_execute_last_data_query()
# #         test_execute_raw_data_query()
# #     else:
# #         # 如果没有异常发生，执行这里的代码
# #         print("代码执行成功")
#
#
# delete_database(session)
# # 关闭session
# session.close()
#
# # 使用完调用putBack归还
# # session_pool.put_back(session)
# # 关闭sessionPool时同时关闭管理的session
# # session_pool.close()
#
#
#
