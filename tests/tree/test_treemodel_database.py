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


# 1、测试创建数据库：set_storage_group
@pytest.mark.usefixtures('fixture_')
def test_database1():
    session.set_storage_group("root.test_database")

# 2、测试删除数据库：delete_storage_group 和 delete_storage_groups
@pytest.mark.usefixtures('fixture_')
def test_database2():
    database_list = []
    for i in range(10):
        session.set_storage_group("root.test_database" + str(i))
        database_list.append("root.test_database" + str(i))
    session.delete_storage_group("root.test_database1")
    session.delete_storage_groups(database_list)

########### 异常情况 #########
# # 1、测试delete_storage_group异常：未创建连接而执行或执行时session重连
# def test_database_error1():
#     with open(config_path, 'r', encoding='utf-8') as file:
#         config = yaml.safe_load(file)
#     session = Session(
#         config['host'],
#         config['port'],
#         user=config['username'],
#         password=config['password'],
#     )
#     session.set_storage_group("root.sg_test_01")
