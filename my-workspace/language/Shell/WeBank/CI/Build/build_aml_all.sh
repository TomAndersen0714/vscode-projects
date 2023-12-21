#!/bin/bash
set -ex

function pace_all() {
#将wtss保存到目录内
  # 获取当前路径 webank-aml/aml_wtss
  CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # 当前路径 webank-aml
  WTSS_DIR="$(cd "${CURRENT_DIR}/aml_wtss/" && pwd)" # 当前路径 aml_wtss
  cd ${WTSS_DIR}

  #根目录下的 ci dist 构建路径
  mkdir -p ${WTSS_DIR}/../dist
  rm -rf ${WTSS_DIR}/../dist/*
  #创建存放原来wtss结构的目录 dist/aml_wtss

  wtss_dist_dir=${CURRENT_DIR}/dist/aml_wtss
  mkdir -p ${wtss_dist_dir}
  #对webank-aml/aml_wtss/target/classes/wtss 的各文件夹移动到规定路径下
  #copy 文件目录
  cp -rf ${WTSS_DIR}/src/main/resources/wtss/* ${WTSS_DIR}/src/main/resources/prod/conf/* ${wtss_dist_dir}/
  echo ${version_no} > ${wtss_dist_dir}/VERSION


#将ddl保存到目录内
  export HIVE_VERSION=${PRODUCT_VERSION}
  export PHOENIX_VERSION=${PRODUCT_VERSION}
  export DQC_VERSION=${PRODUCT_VERSION}

  HIVE_DIR="$(cd "${CURRENT_DIR}/aml_db/" && pwd)" # 当前路径 aml_db
  #hive
  hive_dist_dir=${CURRENT_DIR}/dist/aml_ddl/hive
  mkdir -p ${hive_dist_dir}
  cp -r ${HIVE_DIR}/hive/bin ${hive_dist_dir}/
  cp -r ${HIVE_DIR}/hive/conf ${hive_dist_dir}/
  if [ -d ${HIVE_DIR}/hive/hive/${HIVE_VERSION} ];then
    cp -r ${HIVE_DIR}/hive/hive/${HIVE_VERSION} ${hive_dist_dir}
  fi
  #phoenix
  phoenix_dist_dir=${CURRENT_DIR}/dist/aml_ddl/phoenix
  mkdir -p ${phoenix_dist_dir}
  cp -r ${HIVE_DIR}/phoenix/bin ${phoenix_dist_dir}/
  if [ -d ${HIVE_DIR}/phoenix/${PHOENIX_VERSION} ];then
    cp -r ${HIVE_DIR}/phoenix/${PHOENIX_VERSION} ${phoenix_dist_dir}
  fi

  #dqc
  dqc_dist_dir=${CURRENT_DIR}/dist/aml_ddl/dqc

  mkdir -p ${dqc_dist_dir}
  cp -r ${HIVE_DIR}/dqc/bin ${dqc_dist_dir}/
  cp -r ${HIVE_DIR}/dqc/conf ${dqc_dist_dir}/
  if [ -d ${HIVE_DIR}/dqc/${DQC_VERSION} ];then
    cp -r ${HIVE_DIR}/dqc/${DQC_VERSION} ${dqc_dist_dir}
  fi
  #输出打包信息
  echo "${HIVE_VERSION}" > ${hive_dist_dir}/HIVE_VERSION
  echo "${PHOENIX_VERSION}" > ${phoenix_dist_dir}/PHOENIX_VERSION
  echo "${DQC_VERSION}" > ${dqc_dist_dir}/VERSION

#bdp放入指定目录
  #创建放bdp文件的目录
  bdp_dist_dir=${CURRENT_DIR}/dist/aml_bdp
  mkdir -p ${bdp_dist_dir}

  BDP_DIR="$(cd "${CURRENT_DIR}/aml_bdp/" && pwd)" # 当前路径 aml_bdp
  cp -fr ${BDP_DIR}/* ${bdp_dist_dir}/

  #打包aml-hive和spark包到lib下
  cd ${CURRENT_DIR}/aml_java/back/
  mvn clean install -DskipTests -Pspark-provided -pl aml-client -am -amd
  cp ${CURRENT_DIR}/aml_java/back/aml-client/aml-hive/target/aml-hive.jar ${bdp_dist_dir}/lib/aml_hive.jar
  cp ${CURRENT_DIR}/aml_java/back/aml-client/aml-spark/target/aml-spark.jar ${bdp_dist_dir}/lib/aml-spark.jar


  #打包准实时任务的jar包
  cd ${CURRENT_DIR}/engine_and_udf/nrt-blanca-ext
  mvn clean install -DskipTests -am -amd
  cp ${CURRENT_DIR}/engine_and_udf/nrt-blanca-ext/target/aml-nrt-blanca-ext.jar ${bdp_dist_dir}/lib/aml-nrt-blanca-ext.jar


  ##打包blanca的扩展包
  cd ${CURRENT_DIR}/engine_and_udf/blanca-ext
  mvn clean install -DskipTests -am -amd
  cp ${CURRENT_DIR}/engine_and_udf/blanca-ext/target/aml-blanca-ext.jar ${bdp_dist_dir}/lib/aml-blanca-ext.jar

  ##打包udf的扩展包
  cd ${CURRENT_DIR}/engine_and_udf/udf
  mvn clean install -DskipTests -am -amd
  cp ${CURRENT_DIR}/engine_and_udf/udf/target/aml-udf-ext.jar ${bdp_dist_dir}/lib/aml-udf-ext.jar

  TARGET_DIR=${CURRENT_DIR}/target
  mkdir ${TARGET_DIR}
  PKG_NAME=${SUBSYSTEM_NAME}_bdp_hive_wtss_${PRODUCT_VERSION}_${BUILD_NUMBER}.tar.gz
  cd  ${CURRENT_DIR}/dist/



  tar zcvf ${PKG_NAME} *
  mv ${PKG_NAME} ${CURRENT_DIR}/target/
}

function trim(){
    local trimmed=$1
    trimmed=${trimmed%% }
    trimmed=${trimmed## }
    echo $trimmed
}

#打包全部
if [ "pace" == $1 ];then
    pace_all
fi

