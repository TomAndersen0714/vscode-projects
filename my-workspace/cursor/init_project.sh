#!/bin/bash

# 创建项目目录结构
mkdir -p src/main/java/com/example/cursorplugin
mkdir -p src/main/resources/META-INF

# 创建Java文件
touch src/main/java/com/example/cursorplugin/SqlInJsonLanguageInjector.java

# 创建gradle包装器
gradle wrapper