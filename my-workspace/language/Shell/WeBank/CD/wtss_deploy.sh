#!/bin/bash
CURRENT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_PATH=$1
TARGET_ENV=$2

declare -a NEED_PROJECT_NAMES
NEED_PROJECT_NAMES=("bdp")
#NEED_PROJECT_NAMES=("bdp")
source ${CURRENT_DIR}./wtss_tools/wtss_user.properties
WTSS_USER=${WTSS_USER}
WTSS_PWD=${WTSS_PWD}

for project_name in ${NEED_PROJECT_NAMES[@]};
do
  cp -r ${CURRENT_PATH}/*.properties ${CURRENT_PATH}/${project_name}/
  cd ${CURRENT_PATH}/${project_name}/
  zip -r rrs_aml_${project_name}_wtss.zip *
  cd -
  mv ${CURRENT_PATH}/${project_name}/rrs_aml_${project_name}_wtss.zip ./

  if [ "${TARGET_ENV}" == "grey" ]; then
         #环境2 sit
         WTSS_PWD1=`java -cp wtss_tools/tools/Azkaban-AOMP-2.0/lib/rrs-encrypt-1.0.0.jar:wtss_tools/tools/Azkaban-AOMP-2.0/lib/* cn.webank.rrs.encrypt.RrsEncryptUtil ${WTSS_PWD}|xargs java -cp wtss_tools/tools/Azkaban-AOMP-2.0/lib/Azkaban-AOMP-2.0.jar:wtss_tools/tools/Azkaban-AOMP-2.0/lib/* com.webank.azkaban.utils.RSAUtils |awk -F ':' 'NR==2{print $2}'`
         sh wtss_tools/tools/Azkaban-AOMP-2.0/bin/app-ext.sh -f rrs_aml_${project_name}_wtss.zip -p rrs_aml_${project_name}_wtss  -u${WTSS_USER} -w${WTSS_PWD1}
         if [ $? -ne 0 ]; then
            echo "WTSS发版本异常"
            exit 1
         fi
  else
         #环境1 uat
         WTSS_PWD1=`java -cp wtss_tools/tools/Azkaban-AOMP-1.0/lib/rrs-encrypt-1.0.0.jar:wtss_tools/tools/Azkaban-AOMP-1.0/lib/* cn.webank.rrs.encrypt.RrsEncryptUtil ${WTSS_PWD}|xargs java -cp wtss_tools/tools/Azkaban-AOMP-1.0/lib/Azkaban-AOMP-2.0.jar:wtss_tools/tools/Azkaban-AOMP-1.0/lib/* com.webank.azkaban.utils.RSAUtils |awk -F ':' 'NR==2{print $2}'`
        sh wtss_tools/tools/Azkaban-AOMP-1.0/bin/app-ext.sh -f rrs_aml_${project_name}_wtss.zip -p rrs_aml_${project_name}_wtss  -u${WTSS_USER} -w${WTSS_PWD1}
         if [ $? -ne 0 ]; then
            echo "WTSS发版本异常"
            exit 1
         fi
  fi




#  sh ${DEPLOY_PATH}/../aml_azkaban/bin/push_rrs_aml_wtss.sh rrs_aml_${project_name}_wtss rrs_aml_${project_name}_wtss
  rm rrs_aml_${project_name}_wtss.zip
done
#find ${CURRENT_PATH}/ -name *.zip  -maxdepth 2 | xargs -i rm {}