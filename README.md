# Python 原生接口测试工具使用说明

----

## 环境

- python 3.7+ （测试时使用的是 3.12.9）
- pip3 （测试时使用的是 24.3.1）
- thrift 0.13+ （测试使用的是0.22.0）

## 安装

**仅限首次安装需要，确保python环境已经配置好**

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

确认config配置文件正确：`${python-native-api-testcase}/config.yml`

- 基础自动化测试

```bash
cd ${python-client-test}/tests
pytest
```

**注意：测试用例文件必须以test结尾，方法必须以test开头**

- 代码覆盖率测试

```bash
cd ${python-client-test}/test
pytest --cov=iotdb --cov-report=html --cov-branch --cov-config=.coveragerc
```

生成的报告默认位于程序根目录下`test/htmlcov/`中的index.html 文件

参数说明

- --cov ：指定覆盖率测试目标源码目录（目前会自动取venv依赖库里面的iotdb：venv\Lib\site-packages\iotdb）
- --cov-report：指定覆盖率报告文件格式
- --cov-branch：启用分支测试
- --cov-config：指定覆盖率测试配置文件