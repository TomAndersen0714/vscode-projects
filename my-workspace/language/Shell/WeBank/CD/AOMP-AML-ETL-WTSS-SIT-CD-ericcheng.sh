set -ex

#wtss 工程名
export WTSS_PROJECT_NAME=rrs_aml_bdp_wtss_ericcheng
export TARGET_ENV=grey
export BDP_DEPLOY_PATH_SUFFIX=ericcheng/aml_bdp

#创建部署目录
if [ ! -d "[!DEPLOY_PATH]/[!PKG_NAME]" ]; then
  mkdir -p [!DEPLOY_PATH]/[!PKG_NAME] # 包的名称，如RRS-AMLETL_wtss_main_-3_10.0.6_78.tar.gz

fi

#创建一个临时目录供解压存放使用
if [ ! -d "[!DEPLOY_PATH]/[!PKG_NAME]/[&PACKAGE_TMP_DIR]" ]; then
  mkdir -p [!DEPLOY_PATH]/[!PKG_NAME]/[&PACKAGE_TMP_DIR] 
fi

#解压发布包放到临时目录
tar -zxvf [!PKG_PATH]/[!PKG_NAME] -C [!DEPLOY_PATH]/[!PKG_NAME]/[&PACKAGE_TMP_DIR];
#替换差异化变量
 ( tar -zxvf [!PKG_PATH]/[!CONF_PKG_NAME] -C [!DEPLOY_PATH]/[!PKG_NAME]/[&PACKAGE_TMP_DIR] || echo -n ) ;

#将子目录内容复制到根目录
cp -rf [!DEPLOY_PATH]/[!PKG_NAME]/[&PACKAGE_TMP_DIR]/[&CHILD_PROJECT_DIR_NAME]/* [!DEPLOY_PATH]/[!PKG_NAME];

#删除临时目录
rm -rf [!DEPLOY_PATH]/[!PKG_NAME]/[&PACKAGE_TMP_DIR]
#进入到打包目录
cd [!DEPLOY_PATH]/[!PKG_NAME]

# WTSS上传脚本, 参考 wtss_deploy.sh
wtss_deploy() {
  CURRENT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  DEPLOY_PATH=$1
  TARGET_ENV=$2
  PROJECT_NAME=$3

  declare -a NEED_PROJECT_NAMES
  NEED_PROJECT_NAMES=("bdp")
  #NEED_PROJECT_NAMES=("bdp")
  source ${CURRENT_DIR}./wtss_tools/wtss_user.properties
  WTSS_USER=${WTSS_USER}
  WTSS_PWD=${WTSS_PWD}

  # 修改WTSS配置, 指定bdp-client请求的BDP部署路径后缀, 仅在自测时使用
  sed -i -e "1s|HAML_VERSION=.*|HAML_VERSION=${BDP_DEPLOY_PATH_SUFFIX}|" "${CURRENT_PATH}/sys.properties"

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
          sh wtss_tools/tools/Azkaban-AOMP-2.0/bin/app-ext.sh -f rrs_aml_${project_name}_wtss.zip -p ${PROJECT_NAME}  -u${WTSS_USER} -w${WTSS_PWD1}
          if [ $? -ne 0 ]; then
              echo "WTSS发版本异常"
              exit 1
          fi
    else
          #环境1 uat
          WTSS_PWD1=`java -cp wtss_tools/tools/Azkaban-AOMP-1.0/lib/rrs-encrypt-1.0.0.jar:wtss_tools/tools/Azkaban-AOMP-1.0/lib/* cn.webank.rrs.encrypt.RrsEncryptUtil ${WTSS_PWD}|xargs java -cp wtss_tools/tools/Azkaban-AOMP-1.0/lib/Azkaban-AOMP-2.0.jar:wtss_tools/tools/Azkaban-AOMP-1.0/lib/* com.webank.azkaban.utils.RSAUtils |awk -F ':' 'NR==2{print $2}'`
          sh wtss_tools/tools/Azkaban-AOMP-1.0/bin/app-ext.sh -f rrs_aml_${project_name}_wtss.zip -p ${PROJECT_NAME}  -u${WTSS_USER} -w${WTSS_PWD1}
          if [ $? -ne 0 ]; then
              echo "WTSS发版本异常"
              exit 1
          fi
    fi

  #  sh ${DEPLOY_PATH}/../aml_azkaban/bin/push_rrs_aml_wtss.sh rrs_aml_${project_name}_wtss rrs_aml_${project_name}_wtss
    rm rrs_aml_${project_name}_wtss.zip
  done
  #find ${CURRENT_PATH}/ -name *.zip  -maxdepth 2 | xargs -i rm {}
}


# 上传WTSS项目到WTSS服务端
# sh wtss_deploy.sh [!DEPLOY_PATH] grey
wtss_deploy [!DEPLOY_PATH] ${TARGET_ENV} ${WTSS_PROJECT_NAME}


#清除历史部署包,仅在测试环境使用，生产环境请删除
echo "删除下面目录七天前文件"
cd [!DEPLOY_PATH]
pwd
find ./ -maxdepth  1 ! -path "./"  -atime +7  | xargs -i rm -r {}
echo "删除完成"