#!/bin/bash

# 定义一个变量
# VAR="Hello World"
VAR="Hello"

# 使用if语句检查变量是否等于特定的值
if [ "$VAR" = "Hello World" ]; then
    echo "The variable is equal to Hello World"
else
    echo "The variable is not equal to Hello World"
fi