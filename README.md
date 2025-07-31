# 自动化测试使用说明

----

## 环境

- python 3.7+ （测试时使用的是 3.12.9）
- pip3 （测试时使用的是 24.3.1）
- thrift 0.13+ （测试使用的是0.22.0）

## 安装

**仅限首次安装需要**

1、创建虚拟环境并激活

```bash
python -m venv venv
.\venv\Scripts\activate
```

2、安装需要的依赖

```bash
pip3 install numpy # Example源码需要的依赖
pip3 install pytest # 使用自动化测试需要的
pip3 install pyyaml # 使用yaml配置文件需要的
pip3 install pytest-cov # 测试代码覆盖率需要的
```

3、安装IoTDB依赖

```bash
# 1、拉取源码（已拉取过的只需 git pull 更新下）
git clone https://github.com/apache/iotdb.git
cd iotdb/iotdb-client/client-py
# 2、安装需要的模块（仅限首次需要）
pip3 install build
pip3 install thrift
# 3、编译前确保 maven 和 python3 可用
./release.sh
# 4、引入依赖
cd ${python-native-api-testcase}
pip3 install ${iotdb}/iotdb-client/client-py/dist/apache_iotdb-*.dev0-py3-none-any.whl 
# 卸载之前的：pip3 uninstall apache-iotdb
```

## 使用

- 基础自动化测试

```bash
# 1、确认config配置文件：${python-native-api-testcase}/config.yml

# 2、移到测试目录下
cd ${python-client-test}/tests

# 4、执行测试
pytest
```

**注意：测试用例文件必须以test结尾，方法必须以test开头**

- 代码覆盖率测试

```bash
# 1、在Linux下执行脚本收集要测量覆盖率的目录：${iotdb}/iotdb-client/client-py/iotdb

# 2、收集的源码目录（iotdb）放到测试程序根目录下

# 3、移到测试目录下，执行命令进行代码覆盖率测试并生成报告文件
cd ${python-client-test}/test
pytest --cov=iotdb --cov-report=html --cov-branch
```

生成的报告默认位于程序根目录下`test/htmlcov/`中的index.html 文件