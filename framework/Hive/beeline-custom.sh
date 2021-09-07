#/bin/bash
# 此脚本主要用于配置beeline的默认参数并启动,避免每次启动beeline时都需要
# 手动输入参数

# 默认参数设置
default_params="--showDbInPrompt=true"
# 设置HIVE_BIN_DIR路径
HIVE_BIN_DIR="${HIVE_HOME:-/opt/module/hive-2.3.0}/bin"

# 启动beeline
$HIVE_BIN_DIR/hive --service beeline $default_params "$@"