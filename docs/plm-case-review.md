# Python 原生接口测试用例审查（PLM 导入前）

- 测试程序：`python-native-api-testcase`（开源版）
- 仓库：`https://github.com/thulab/python-native-api-testcase.git`
- 分支：`feat/plm-onboarding`
- PLM 自动化类型：`session-python`（按 111 开源版口径，企业版本轮不接）
- 权威用例来源：111 `native_api_test_report/python/report.html`（88 tests / 88 passed）；`.plm-case-defs.json` 88 条
- 说明：本文件用于导入前人工审查；有分歧的条目在“待审查”列标注。

## 一、导入用例（88 条）

| # | pytest 目标 | PLM 用例名 | 功能点 | 描述 | 待审查 |
|---|---|---|---|---|---|
| 1 | `table/test_tablemodel_insert.py::test_insert1` | Python 开源｜表模型-写入：测试一般写入数据 | 接口模块/Python 原生接口/表模型-写入 | 测试一般写入数据 | 无 |
| 2 | `table/test_tablemodel_insert.py::test_insert2` | Python 开源｜表模型-写入：测试Numpy_tablet写入数据 | 接口模块/Python 原生接口/表模型-写入 | 测试Numpy_tablet写入数据 | 无 |
| 3 | `table/test_tablemodel_insert.py::test_insert3` | Python 开源｜表模型-写入：测试直接写入数据 | 接口模块/Python 原生接口/表模型-写入 | 测试直接写入数据 | 无 |
| 4 | `table/test_tablemodel_insert.py::test_insert4` | Python 开源｜表模型-写入：乱序写入：测试时间戳自动排序功能 | 接口模块/Python 原生接口/表模型-写入 | 乱序写入：测试时间戳自动排序功能 | 无 |
| 5 | `table/test_tablemodel_insert.py::test_insert_error1` | Python 开源｜表模型-写入：错误情况：时间戳长度和值长度不一致 | 接口模块/Python 原生接口/表模型-写入 | 错误情况：时间戳长度和值长度不一致 | 无 |
| 6 | `table/test_tablemodel_query.py::test_query1` | Python 开源｜表模型-查询：测试SessionDataSet：验证列类型和数据类型以及行数的正确性 | 接口模块/Python 原生接口/表模型-查询 | 测试SessionDataSet：验证列类型和数据类型以及行数的正确性 | 无 |
| 7 | `table/test_tablemodel_query.py::test_query2` | Python 开源｜表模型-查询：测试查询分批返回 DataFrame 功能：当 SessionDataSet 总行数超过 fetchSize 时 | 接口模块/Python 原生接口/表模型-查询 | 测试查询分批返回 DataFrame 功能：当 SessionDataSet 总行数超过 fetchSize 时 | 无 |
| 8 | `table/test_tablemodel_query.py::test_query3` | Python 开源｜表模型-查询：测试查询分批返回 DataFrame 功能：当 SessionDataSet 剩余的行数小于 fetchSize 时 | 接口模块/Python 原生接口/表模型-查询 | 测试查询分批返回 DataFrame 功能：当 SessionDataSet 剩余的行数小于 fetchSize 时 | 无 |
| 9 | `table/test_tablemodel_query.py::test_query4` | Python 开源｜表模型-查询：测试查询分批返回 DataFrame 功能：当 SessionDataSet 剩余的行数等于 fetchSize 时 | 接口模块/Python 原生接口/表模型-查询 | 测试查询分批返回 DataFrame 功能：当 SessionDataSet 剩余的行数等于 fetchSize 时 | 无 |
| 10 | `table/test_tablemodel_query.py::test_query5` | Python 开源｜表模型-查询：测试 fetchSize <= 0 的异常：Session 打印一个警告日志 ，fetchSize 重置为默认值 5000 | 接口模块/Python 原生接口/表模型-查询 | 测试 fetchSize <= 0 的异常：Session 打印一个警告日志 ，fetchSize 重置为默认值 5000 | 无 |
| 11 | `table/test_tablemodel_query.py::test_fields1` | Python 开源｜表模型-查询：测试fields查询：验证类型 | 接口模块/Python 原生接口/表模型-查询 | 测试fields查询：验证类型 | 无 |
| 12 | `table/test_tablemodel_query.py::test_fields2` | Python 开源｜表模型-查询：测试fields查询：验证字段值 | 接口模块/Python 原生接口/表模型-查询 | 测试fields查询：验证字段值 | 无 |
| 13 | `table/test_tablemodel_query.py::test_fields3` | Python 开源｜表模型-查询：测试fields查询：测试is_null、copy和set_xxx_value等方法有效性 | 接口模块/Python 原生接口/表模型-查询 | 测试fields查询：测试is_null、copy和set_xxx_value等方法有效性 | 无 |
| 14 | `table/test_tablemodel_query.py::test_fields_error1` | Python 开源｜表模型-查询：测试fields查询， get_XXX_valu e异常情况 | 接口模块/Python 原生接口/表模型-查询 | 测试fields查询， get_XXX_valu e异常情况 | 无 |
| 15 | `table/test_tablemodel_query.py::test_query_error1` | Python 开源｜表模型-查询：测试 StatementExecutionException 异常 | 接口模块/Python 原生接口/表模型-查询 | 测试 StatementExecutionException 异常 | 无 |
| 16 | `table/test_tablemodel_session.py::test_session1` | Python 开源｜表模型-会话：测试最简 session 连接路径：host/port 来自配置，凭据显式传入（PLM TimechoDB 默认口令非 root/root）。 | 接口模块/Python 原生接口/表模型-会话 | 测试最简 session 连接路径：host/port 来自配置，凭据显式传入（PLM TimechoDB 默认口令非 root/root）。 | 无 |
| 17 | `table/test_tablemodel_session.py::test_session2` | Python 开源｜表模型-会话：测试最全 session 连接：session指定数据库 | 接口模块/Python 原生接口/表模型-会话 | 测试最全 session 连接：session指定数据库 | 无 |
| 18 | `table/test_tablemodel_session.py::test_session3` | Python 开源｜表模型-会话：测试 enable_redirection 参数：关闭重定向 | 接口模块/Python 原生接口/表模型-会话 | 测试 enable_redirection 参数：关闭重定向 | 无 |
| 19 | `table/test_tablemodel_session.py::test_session4` | Python 开源｜表模型-会话：测试 enable_compression 参数：开启RPC压缩（需要单独测试，IoTDB需要开启对应配置） | 接口模块/Python 原生接口/表模型-会话 | 测试 enable_compression 参数：开启RPC压缩（需要单独测试，IoTDB需要开启对应配置） | 无 |
| 20 | `table/test_tablemodel_session.py::test_session5` | Python 开源｜表模型-会话：测试 use_ssl 参数：开启SSL连接（需要单独测试，生成并存放密钥） | 接口模块/Python 原生接口/表模型-会话 | 测试 use_ssl 参数：开启SSL连接（需要单独测试，生成并存放密钥） | 无 |
| 21 | `table/test_tablemodel_session.py::test_session6` | Python 开源｜表模型-会话：测试 connection_timeout_in_ms 参数 | 接口模块/Python 原生接口/表模型-会话 | 测试 connection_timeout_in_ms 参数 | 无 |
| 22 | `table/test_tablemodel_session.py::test_session7` | Python 开源｜表模型-会话：测试多次 Close | 接口模块/Python 原生接口/表模型-会话 | 测试多次 Close | 无 |
| 23 | `table/test_tablemodel_session.py::test_session_error1` | Python 开源｜表模型-会话：IoTDBConnectionException异常：未实现 | 接口模块/Python 原生接口/表模型-会话 | IoTDBConnectionException异常：未实现 | 无 |
| 24 | `table/test_tablemodel_session_pool.py::test_session1` | Python 开源｜表模型-会话池：测试最简 SessionPool 连接 | 接口模块/Python 原生接口/表模型-会话池 | 测试最简 SessionPool 连接 | 无 |
| 25 | `table/test_tablemodel_session_pool.py::test_session2` | Python 开源｜表模型-会话池：测试最全 session 连接：session指定数据库 | 接口模块/Python 原生接口/表模型-会话池 | 测试最全 session 连接：session指定数据库 | 无 |
| 26 | `table/test_tablemodel_session_pool.py::test_session3` | Python 开源｜表模型-会话池：测试 enable_redirection 参数：关闭重定向 | 接口模块/Python 原生接口/表模型-会话池 | 测试 enable_redirection 参数：关闭重定向 | 无 |
| 27 | `table/test_tablemodel_session_pool.py::test_session4` | Python 开源｜表模型-会话池：测试 enable_compression 参数：开启RPC压缩（需要单独测试，IoTDB需要开启对应配置） | 接口模块/Python 原生接口/表模型-会话池 | 测试 enable_compression 参数：开启RPC压缩（需要单独测试，IoTDB需要开启对应配置） | 无 |
| 28 | `table/test_tablemodel_session_pool.py::test_session5` | Python 开源｜表模型-会话池：测试 use_ssl 参数：开启SSL连接（需要单独测试，生成并存放密钥） | 接口模块/Python 原生接口/表模型-会话池 | 测试 use_ssl 参数：开启SSL连接（需要单独测试，生成并存放密钥） | 无 |
| 29 | `table/test_tablemodel_session_pool.py::test_session6` | Python 开源｜表模型-会话池：测试 connection_timeout_in_ms 参数 | 接口模块/Python 原生接口/表模型-会话池 | 测试 connection_timeout_in_ms 参数 | 无 |
| 30 | `table/test_tablemodel_session_pool.py::test_session8` | Python 开源｜表模型-会话池：测试 max_retry 参数：0, 1, 100, 10000 | 接口模块/Python 原生接口/表模型-会话池 | 测试 max_retry 参数：0, 1, 100, 10000 | 无 |
| 31 | `table/test_tablemodel_session_pool.py::test_session9` | Python 开源｜表模型-会话池：测试 max_pool_size 参数：0, 1, 100, 10000 | 接口模块/Python 原生接口/表模型-会话池 | 测试 max_pool_size 参数：0, 1, 100, 10000 | 无 |
| 32 | `tree/test_treemodel_database.py::test_database1` | Python 开源｜树模型-数据库：测试创建数据库：set_storage_group | 接口模块/Python 原生接口/树模型-数据库 | 测试创建数据库：set_storage_group | 无 |
| 33 | `tree/test_treemodel_database.py::test_database2` | Python 开源｜树模型-数据库：测试删除数据库：delete_storage_group 和 delete_storage_groups | 接口模块/Python 原生接口/树模型-数据库 | 测试删除数据库：delete_storage_group 和 delete_storage_groups | 无 |
| 34 | `tree/test_treemodel_database.py::test_database_error1` | Python 开源｜树模型-数据库：测试delete_storage_group异常：未创建连接而执行或执行时session重连 | 接口模块/Python 原生接口/树模型-数据库 | 测试delete_storage_group异常：未创建连接而执行或执行时session重连 | 无 |
| 35 | `tree/test_treemodel_insert.py::test_insert_tablet` | Python 开源｜树模型-写入：测试往非对齐时间序列写入一条 tablet 数据 | 接口模块/Python 原生接口/树模型-写入 | 测试往非对齐时间序列写入一条 tablet 数据 | 无 |
| 36 | `tree/test_treemodel_insert.py::test_insert_aligned_tablet` | Python 开源｜树模型-写入：测试往对齐时间序列写入一条 tablet 数据 | 接口模块/Python 原生接口/树模型-写入 | 测试往对齐时间序列写入一条 tablet 数据 | 无 |
| 37 | `tree/test_treemodel_insert.py::test_insert_tablets` | Python 开源｜树模型-写入：测试往非对齐时间序列写入多条 tablet 数据 | 接口模块/Python 原生接口/树模型-写入 | 测试往非对齐时间序列写入多条 tablet 数据 | 无 |
| 38 | `tree/test_treemodel_insert.py::test_insert_aligned_tablets` | Python 开源｜树模型-写入：测试往对齐时间序列写入多条 tablet 数据 | 接口模块/Python 原生接口/树模型-写入 | 测试往对齐时间序列写入多条 tablet 数据 | 无 |
| 39 | `tree/test_treemodel_insert.py::test_insert_record` | Python 开源｜树模型-写入：测试往非对齐时间序列写入一条 Record 数据 | 接口模块/Python 原生接口/树模型-写入 | 测试往非对齐时间序列写入一条 Record 数据 | 无 |
| 40 | `tree/test_treemodel_insert.py::test_insert_aligned_record` | Python 开源｜树模型-写入：测试往对齐时间序列写入一条 Record 数据 | 接口模块/Python 原生接口/树模型-写入 | 测试往对齐时间序列写入一条 Record 数据 | 无 |
| 41 | `tree/test_treemodel_insert.py::test_insert_records` | Python 开源｜树模型-写入：测试往非对齐时间序列写入多条 Record 数据 | 接口模块/Python 原生接口/树模型-写入 | 测试往非对齐时间序列写入多条 Record 数据 | 无 |
| 42 | `tree/test_treemodel_insert.py::test_insert_aligned_records` | Python 开源｜树模型-写入：测试往对齐时间序列写入多条 Record 数据 | 接口模块/Python 原生接口/树模型-写入 | 测试往对齐时间序列写入多条 Record 数据 | 无 |
| 43 | `tree/test_treemodel_insert.py::test_insert_records_of_one_device` | Python 开源｜树模型-写入：测试插入同属于一个非对齐 device 的多个 Record 数据 | 接口模块/Python 原生接口/树模型-写入 | 测试插入同属于一个非对齐 device 的多个 Record 数据 | 无 |
| 44 | `tree/test_treemodel_insert.py::test_insert_aligned_records_of_one_device` | Python 开源｜树模型-写入：测试插入同属于一个对齐 device 的多个 Record 数据 | 接口模块/Python 原生接口/树模型-写入 | 测试插入同属于一个对齐 device 的多个 Record 数据 | 无 |
| 45 | `tree/test_treemodel_insert.py::test_insert_str_record` | Python 开源｜树模型-写入：测试带有类型推断的写入：写入单条记录 | 接口模块/Python 原生接口/树模型-写入 | 测试带有类型推断的写入：写入单条记录 | 无 |
| 46 | `tree/test_treemodel_insert.py::test_insert_aligned_str_record` | Python 开源｜树模型-写入：测试带有类型推断的写入：写入单条对齐记录 | 接口模块/Python 原生接口/树模型-写入 | 测试带有类型推断的写入：写入单条对齐记录 | 无 |
| 47 | `tree/test_treemodel_insert.py::test_insert_string_records_of_one_device` | Python 开源｜树模型-写入：测试带有类型推断的写入：往一个设备中写入多条记录 | 接口模块/Python 原生接口/树模型-写入 | 测试带有类型推断的写入：往一个设备中写入多条记录 | 无 |
| 48 | `tree/test_treemodel_insert.py::test_insert_aligned_string_records_of_one_device` | Python 开源｜树模型-写入：测试带有类型推断的写入：往一个设备中写入多条对齐记录 | 接口模块/Python 原生接口/树模型-写入 | 测试带有类型推断的写入：往一个设备中写入多条对齐记录 | 无 |
| 49 | `tree/test_treemodel_insert.py::test_insert_direct` | Python 开源｜树模型-写入：测试直接写入 | 接口模块/Python 原生接口/树模型-写入 | 测试直接写入 | 无 |
| 50 | `tree/test_treemodel_insert.py::test_delete_data1` | Python 开源｜树模型-写入：测试删除多个时间序列全部的数据 | 接口模块/Python 原生接口/树模型-写入 | 测试删除多个时间序列全部的数据 | 无 |
| 51 | `tree/test_treemodel_insert.py::test_delete_data2` | Python 开源｜树模型-写入：测试删除多个时间序列指定范围的数据（删除全部数据） | 接口模块/Python 原生接口/树模型-写入 | 测试删除多个时间序列指定范围的数据（删除全部数据） | 无 |
| 52 | `tree/test_treemodel_insert.py::test_check_sorted` | Python 开源｜树模型-写入：测试检查顺序函数：check_sorted | 接口模块/Python 原生接口/树模型-写入 | 测试检查顺序函数：check_sorted | 无 |
| 53 | `tree/test_treemodel_insert.py::test_insert_string_records_of_one_device_error` | Python 开源｜树模型-写入：测试带有类型推断的写入：往一个设备中写入多条记录 | 接口模块/Python 原生接口/树模型-写入 | 测试带有类型推断的写入：往一个设备中写入多条记录 | 无 |
| 54 | `tree/test_treemodel_insert.py::test_insert_aligned_string_records_of_one_device_error` | Python 开源｜树模型-写入：测试带有类型推断的写入：往一个设备中写入多条对齐记录 | 接口模块/Python 原生接口/树模型-写入 | 测试带有类型推断的写入：往一个设备中写入多条对齐记录 | 无 |
| 55 | `tree/test_treemodel_insert.py::test_insert_records_error` | Python 开源｜树模型-写入：测试insert_records数量不一致情况 | 接口模块/Python 原生接口/树模型-写入 | 测试insert_records数量不一致情况 | 无 |
| 56 | `tree/test_treemodel_insert.py::test_insert_str_record_error` | Python 开源｜树模型-写入：测试 insert_str_record 数量不一致情况 | 接口模块/Python 原生接口/树模型-写入 | 测试 insert_str_record 数量不一致情况 | 无 |
| 57 | `tree/test_treemodel_insert.py::test_insert_record_error` | Python 开源｜树模型-写入：测试 insert_record 数量不一致情况 | 接口模块/Python 原生接口/树模型-写入 | 测试 insert_record 数量不一致情况 | 无 |
| 58 | `tree/test_treemodel_query.py::test_execute_query_statement` | Python 开源｜树模型-查询：测试 execute_query_statement 函数 | 接口模块/Python 原生接口/树模型-查询 | 测试 execute_query_statement 函数 | 无 |
| 59 | `tree/test_treemodel_query.py::test_execute_raw_data_query` | Python 开源｜树模型-查询：测试 execute_raw_data_query 函数 | 接口模块/Python 原生接口/树模型-查询 | 测试 execute_raw_data_query 函数 | 无 |
| 60 | `tree/test_treemodel_query.py::test_execute_last_data_query` | Python 开源｜树模型-查询：测试 execute_last_data_query 函数 | 接口模块/Python 原生接口/树模型-查询 | 测试 execute_last_data_query 函数 | 无 |
| 61 | `tree/test_treemodel_session.py::test_session1` | Python 开源｜树模型-会话：测试session连接：最简构造 | 接口模块/Python 原生接口/树模型-会话 | 测试session连接：最简构造 | 无 |
| 62 | `tree/test_treemodel_session.py::test_session2` | Python 开源｜树模型-会话：测试session连接：最繁构造 | 接口模块/Python 原生接口/树模型-会话 | 测试session连接：最繁构造 | 无 |
| 63 | `tree/test_treemodel_session.py::test_session3` | Python 开源｜树模型-会话：测试session连接：多次open | 接口模块/Python 原生接口/树模型-会话 | 测试session连接：多次open | 无 |
| 64 | `tree/test_treemodel_session.py::test_session4` | Python 开源｜树模型-会话：测试session连接：多次close | 接口模块/Python 原生接口/树模型-会话 | 测试session连接：多次close | 无 |
| 65 | `tree/test_treemodel_session.py::test_session5` | Python 开源｜树模型-会话：测试session连接：close后再open | 接口模块/Python 原生接口/树模型-会话 | 测试session连接：close后再open | 无 |
| 66 | `tree/test_treemodel_session.py::test_session6` | Python 开源｜树模型-会话：测试超时连接：需要手动测试，不清楚具体多久会超时 | 接口模块/Python 原生接口/树模型-会话 | 测试超时连接：需要手动测试，不清楚具体多久会超时 | 无 |
| 67 | `tree/test_treemodel_session.py::test_session7` | Python 开源｜树模型-会话：测试Session的SSL连接：需要手动测试，测试时请将use_ssl设置为True，并开启iotdb对应配置 | 接口模块/Python 原生接口/树模型-会话 | 测试Session的SSL连接：需要手动测试，测试时请将use_ssl设置为True，并开启iotdb对应配置 | 无 |
| 68 | `tree/test_treemodel_session.py::test_session8` | Python 开源｜树模型-会话：测试session的get_time_zone和set_time_zone函数 | 接口模块/Python 原生接口/树模型-会话 | 测试session的get_time_zone和set_time_zone函数 | 无 |
| 69 | `tree/test_treemodel_session.py::test_session9` | Python 开源｜树模型-会话：测试session的 execute_statement 函数 | 接口模块/Python 原生接口/树模型-会话 | 测试session的 execute_statement 函数 | 无 |
| 70 | `tree/test_treemodel_session.py::test_session10` | Python 开源｜树模型-会话：测试session的 execute_non_query_statement 和 execute_query_statement 函数 | 接口模块/Python 原生接口/树模型-会话 | 测试session的 execute_non_query_statement 和 execute_query_statement 函数 | 无 |
| 71 | `tree/test_treemodel_session.py::test_session_error2` | Python 开源｜树模型-会话：测试session的 execute_non_query_statement函数异常情况 | 接口模块/Python 原生接口/树模型-会话 | 测试session的 execute_non_query_statement函数异常情况 | 无 |
| 72 | `tree/test_treemodel_session.py::test_session_error3` | Python 开源｜树模型-会话：测试session的 execute_query_statement函数异常情况 | 接口模块/Python 原生接口/树模型-会话 | 测试session的 execute_query_statement函数异常情况 | 无 |
| 73 | `tree/test_treemodel_session.py::test_session_error4` | Python 开源｜树模型-会话：测试session的 execute_statement函数异常情况 | 接口模块/Python 原生接口/树模型-会话 | 测试session的 execute_statement函数异常情况 | 无 |
| 74 | `tree/test_treemodel_session.py::test_session_error5` | Python 开源｜树模型-会话：测试session的set_time_zone函数异常情况 | 接口模块/Python 原生接口/树模型-会话 | 测试session的set_time_zone函数异常情况 | 无 |
| 75 | `tree/test_treemodel_session_pool.py::test_session_pool1` | Python 开源｜树模型-会话池：测试session_pool：最简构造 | 接口模块/Python 原生接口/树模型-会话池 | 测试session_pool：最简构造 | 无 |
| 76 | `tree/test_treemodel_session_pool.py::test_session_pool2` | Python 开源｜树模型-会话池：测试session_pool：最繁构造 | 接口模块/Python 原生接口/树模型-会话池 | 测试session_pool：最繁构造 | 无 |
| 77 | `tree/test_treemodel_session_pool.py::test_session_pool3` | Python 开源｜树模型-会话池：测试SessionPool的SSL连接 | 接口模块/Python 原生接口/树模型-会话池 | 测试SessionPool的SSL连接 | 无 |
| 78 | `tree/test_treemodel_session_pool.py::test_session_pool4` | Python 开源｜树模型-会话池：测试session_pool：无node_urls | 接口模块/Python 原生接口/树模型-会话池 | 测试session_pool：无node_urls | 无 |
| 79 | `tree/test_treemodel_session_pool.py::test_session_pool5` | Python 开源｜树模型-会话池：测试session_pool：push_back的session未open | 接口模块/Python 原生接口/树模型-会话池 | 测试session_pool：push_back的session未open | 无 |
| 80 | `tree/test_treemodel_session_pool.py::test_session_pool_error1` | Python 开源｜树模型-会话池：测试session_pool：node_urls为None | 接口模块/Python 原生接口/树模型-会话池 | 测试session_pool：node_urls为None | 无 |
| 81 | `tree/test_treemodel_session_pool.py::test_session_pool_error2` | Python 开源｜树模型-会话池：测试session_pool：get_session前关闭session_pool | 接口模块/Python 原生接口/树模型-会话池 | 测试session_pool：get_session前关闭session_pool | 无 |
| 82 | `tree/test_treemodel_session_pool.py::test_session_pool_error3` | Python 开源｜树模型-会话池：测试session_pool：put_back的session_pool已经close | 接口模块/Python 原生接口/树模型-会话池 | 测试session_pool：put_back的session_pool已经close | 无 |
| 83 | `tree/test_treemodel_session_pool.py::test_session_pool_error4` | Python 开源｜树模型-会话池：测试session_pool：创建session_pool时，max_pool_size <= 0 | 接口模块/Python 原生接口/树模型-会话池 | 测试session_pool：创建session_pool时，max_pool_size <= 0 | 无 |
| 84 | `tree/test_treemodel_timeseries.py::test_timeseries1` | Python 开源｜树模型-时间序列：测试创建非对齐时间序列：create_time_series | 接口模块/Python 原生接口/树模型-时间序列 | 测试创建非对齐时间序列：create_time_series | 无 |
| 85 | `tree/test_treemodel_timeseries.py::test_aligned_timeseries1` | Python 开源｜树模型-时间序列：测试创建对齐时间序列：create_aligned_time_series | 接口模块/Python 原生接口/树模型-时间序列 | 测试创建对齐时间序列：create_aligned_time_series | 无 |
| 86 | `tree/test_treemodel_timeseries.py::test_multi_timeseries1` | Python 开源｜树模型-时间序列：测试创建多个非对齐时间序列 | 接口模块/Python 原生接口/树模型-时间序列 | 测试创建多个非对齐时间序列 | 无 |
| 87 | `tree/test_treemodel_timeseries.py::test_delete_timeseries1` | Python 开源｜树模型-时间序列：测试删除时间序列：session.delete_time_series() | 接口模块/Python 原生接口/树模型-时间序列 | 测试删除时间序列：session.delete_time_series() | 无 |
| 88 | `tree/test_treemodel_timeseries.py::test_check_timeseries_exists1` | Python 开源｜树模型-时间序列：测试检查时间序列是否存在：check_time_series_exists | 接口模块/Python 原生接口/树模型-时间序列 | 测试检查时间序列是否存在：check_time_series_exists | 无 |

## 二、审查结论

- 88 条用例名称与功能点路径按测试文件分组生成，覆盖表模型写入/查询/会话/会话池与树模型数据库/时间序列/写入/查询/会话/会话池。
- 111 报告 88/88 通过，作为基线。
- 依赖版本：111 为 Python 3.10 + numpy 1.25.2 / pandas 2.0.3 / greenlet 2.0.2；76 为 Python 3.12，上述版本无 cp312 wheel，改用兼容的 numpy 1.26.4 / pandas 2.1.4 / greenlet 3.0.3，并以 88 条实跑结果为准。客户端 apache-iotdb 2.0.11（PLM 分支默认）。
- 本轮无命名分歧；如后续发现用例名与测试内容不符，在本文件追加“待审查”记录。
- 环境差异记录：111 为开源版默认 root/root；PLM 的 TimechoDB 实例默认口令为 TimechoDB@2021。`tree/test_treemodel_session.py::test_session1` 原用 2 参构造（默认 root/root）会 801，已改为显式传 config 凭据，用例名同步改为“最简 session 连接（host/port + 显式凭据）”；`table/test_tablemodel_session_pool.py::test_session1` 同理改为显式传 node_urls/凭据。

## 三、PLM 环境验证（2026-11-05）

- **8GB 手动实例**（`py-large-test`，TimechoDB 2.0.10.2，192.168.99.51:30891）：88 条全量 **88 passed in 742.91s**。
- **2GB Native 模板实例**：`test_query5` 报 `719: There is not enough memory...`（剩余 31MB，需要 37MB）；树模型插入用例出现连接中断。
- 结论：Python 全量需要把产品 Native 模板实例内存提高到 **8GB（至少 6GB）** 并重新检验；2GB 仅够轻量用例。
- 此前失败的 11 条（`test_query5`、`table/test_tablemodel_session_pool.py::test_session1`、树模型插入 9 条）在 8GB 实例上全部通过。
