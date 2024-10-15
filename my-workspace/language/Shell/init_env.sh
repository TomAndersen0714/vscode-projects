#!/bin/bash

# 创建Python虚拟环境
python -m venv myenv

# 加载虚拟环境
source myenv/bin/activate

# 配置PIP源
pip config set --local global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
pip config set --local global.trusted-host pypi.tuna.tsinghua.edu.cn

# 安装项目依赖
pip install -r requirements.txt

