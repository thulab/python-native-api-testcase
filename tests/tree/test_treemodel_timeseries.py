from datetime import date
import pytest
import yaml
from iotdb.Session import Session
from iotdb.SessionPool import PoolConfig, SessionPool
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet

# 配置文件目录
config_path = "../conf/config.yml"


# 读取配置文件
def read_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


# 测试 fixture：测试环境清理
@pytest.fixture()
def fixture_():
    global session
    global session_pool
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)

        if config['enable_session_pool']:
            pool_config = PoolConfig(
                node_urls=config['node_urls'],
                user_name=config['username'],
                password=config['password'],
            )
            max_pool_size = 5
            wait_timeout_in_ms = 3000
            session_pool = SessionPool(pool_config, max_pool_size, wait_timeout_in_ms)
            session = session_pool.get_session()
        else:
            session = Session(
                config['host'],
                config['port'],
                config['username'],
                config['password'],
            )

        session.open(False)

        # 清理环境
        session.execute_non_query_statement("DELETE DATABASE root.**")

    except Exception as e:
        assert False, "There is an error:" + str(e)

    yield
    # 用例执行完成后清理环境代码
    try:
        session_data_set = session.execute_query_statement("show databases")
        if session_data_set.has_next():
            session.execute_non_query_statement("DELETE DATABASE root.**")

        if config['enable_session_pool']:
            session_pool.put_back(session)
            session_pool.close()
        else:
            session.close()

    except Exception as e:
        assert False, "There is an error:" + str(e)

# 1、测试创建非对齐时间序列：create_time_series
@pytest.mark.usefixtures('fixture_')
def test_timeseries1():
    database_name_ = "root.test_timeseries"
    device_name_ = database_name_ + "." + "d1"
    session.create_time_series(device_name_ + "." + "BOOOLEAN", TSDataType.BOOLEAN, TSEncoding.PLAIN,Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series(device_name_ + "." + "INT32", TSDataType.INT32, TSEncoding.PLAIN,Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series(device_name_ + "." + "INT64", TSDataType.INT64, TSEncoding.PLAIN,Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series(device_name_ + "." + "FLOAT", TSDataType.FLOAT, TSEncoding.PLAIN,Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series(device_name_ + "." + "DOUBLE", TSDataType.DOUBLE, TSEncoding.PLAIN,Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series(device_name_ + "." + "TEXT", TSDataType.TEXT, TSEncoding.PLAIN,Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series(device_name_ + "." + "STRING", TSDataType.STRING, TSEncoding.PLAIN,Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series(device_name_ + "." + "BLOB", TSDataType.BLOB, TSEncoding.PLAIN,Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series(device_name_ + "." + "DATE", TSDataType.DATE, TSEncoding.PLAIN,Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)
    session.create_time_series(device_name_ + "." + "TS", TSDataType.TIMESTAMP, TSEncoding.PLAIN,Compressor.LZ4,
                               props=None, tags=None, attributes=None, alias=None)


# 2、测试创建对齐时间序列：create_aligned_time_series
@pytest.mark.usefixtures('fixture_')
def test_aligned_timeseries1():
    database_name_ = "root.test_timeseries"
    device_name_ = database_name_ + "." + "d1"
    measurements_ = [
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "STRING", "BLOB", "DATE", "TS"
    ]
    data_type_lst_ = [
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
        TSDataType.STRING,
        TSDataType.BLOB,
        TSDataType.DATE,
        TSDataType.TIMESTAMP
    ]
    session.set_storage_group(database_name_)
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(measurements_))]
    compressor_lst_ = [Compressor.LZ4 for _ in range(len(measurements_))]
    session.create_aligned_time_series(device_name_, measurements_, data_type_lst_, encoding_lst_, compressor_lst_)

# 3、测试创建多个非对齐时间序列
@pytest.mark.usefixtures('fixture_')
def test_multi_timeseries1():
    database_name_ = "root.test_session"
    device_name_ = database_name_ + "." + "d1"
    measurements_ = [
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "STRING", "BLOB", "DATE", "TS"
    ]
    data_type_lst_ = [
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
        TSDataType.STRING,
        TSDataType.BLOB,
        TSDataType.DATE,
        TSDataType.TIMESTAMP
    ]
    session.set_storage_group(database_name_)
    ts_path_lst_ = [device_name_ + "." + measurements_[i] for i in range(len(measurements_))]
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(measurements_))]
    compressor_lst_ = [Compressor.LZ4 for _ in range(len(measurements_))]
    session.create_multi_time_series(
        ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
    )

# 4、测试删除时间序列：session.delete_time_series()
@pytest.mark.usefixtures('fixture_')
def test_delete_timeseries1():
    database_name_ = "root.test_session"
    device_name_ = database_name_ + "." + "d1"
    measurements_ = [
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "STRING", "BLOB", "DATE", "TS"
    ]
    data_type_lst_ = [
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
        TSDataType.STRING,
        TSDataType.BLOB,
        TSDataType.DATE,
        TSDataType.TIMESTAMP
    ]
    session.set_storage_group(database_name_)
    ts_path_lst_ = [device_name_ + "." + measurements_[i] for i in range(len(measurements_))]
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(measurements_))]
    compressor_lst_ = [Compressor.LZ4 for _ in range(len(measurements_))]
    session.create_multi_time_series(
        ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
    )
    session.delete_time_series(ts_path_lst_)
    session.create_aligned_time_series(device_name_, measurements_, data_type_lst_, encoding_lst_, compressor_lst_)
    session.delete_time_series(ts_path_lst_)

# 5、测试检查时间序列是否存在：check_time_series_exists
@pytest.mark.usefixtures('fixture_')
def test_check_timeseries_exists1():
    database_name_ = "root.test_session"
    device_name_ = database_name_ + "." + "d1"
    measurements_ = [
        "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "STRING", "BLOB", "DATE", "TS"
    ]
    data_type_lst_ = [
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
        TSDataType.STRING,
        TSDataType.BLOB,
        TSDataType.DATE,
        TSDataType.TIMESTAMP
    ]
    session.set_storage_group(database_name_)
    ts_path_lst_ = [device_name_ + "." + measurements_[i] for i in range(len(measurements_))]
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(measurements_))]
    compressor_lst_ = [Compressor.LZ4 for _ in range(len(measurements_))]
    # 非对齐时间序列
    session.create_multi_time_series(
        ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
    )
    for path in ts_path_lst_:
        assert session.check_time_series_exists(path), "与预期不符，未检查到时间序列"
    # 对齐时间序列
    device_name_ = database_name_ + ".d2"
    ts_path_lst_ = [device_name_ + "." + measurements_[i] for i in range(len(measurements_))]
    session.create_aligned_time_series(device_name_, measurements_, data_type_lst_, encoding_lst_, compressor_lst_)
    for path in ts_path_lst_:
        assert session.check_time_series_exists(path), "与预期不符，未检查到时间序列"
