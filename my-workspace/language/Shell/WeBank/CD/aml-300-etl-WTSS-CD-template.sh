set -ex

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

#将BDP的目录内容复制到包名内
cp -rf [!DEPLOY_PATH]/[!PKG_NAME]/[&PACKAGE_TMP_DIR]/[&CHILD_PROJECT_DIR_NAME]/* [!DEPLOY_PATH]/[!PKG_NAME];

#删除临时目录
rm -rf [!DEPLOY_PATH]/[!PKG_NAME]/[&PACKAGE_TMP_DIR]
#进入到打包目录
cd [!DEPLOY_PATH]/[!PKG_NAME]

#`sed -i "s/ab0cc032bf95ef29c48d6e1a2c503d13a9e9bebf6034d919a9049f6dab5422468b8fa1097366319e6b435c4874a2e2f7486b310bbc49dee9861facd783233bf950ca17af9dc86fee9e1e7cef1e7a49d85aa7f0588e5356e47240e6641546f194e6bbecade813620d124636f8e4b999baa5282ab23ca616a73cc145997da882e1112b183088b2f98d6a3346b594256f3ac2a074cac504c6cff4646bd4f9fa27aea2c930b8b93ebfa36e7be88ef16560ffde2707f2ee0de002babc58b87701d17942e5d97ff8295fc50afe464fc041b1e78bf9c43f2f6b20d749595a2717bf4c2f6ae36370a782191b07c2bd35931cf266aae4567c0e6c37cec5fcd7a581d9dcdd/be08e579eff3704bf621cfbb3076c6236c428768969df904a0f642c13a194ee2e867b3a147a0cf7cb7cf6bd837b765a40fec154fb292ba641d48478fae142641fed1ca502441bd3accd1f451022e5c529e60986f9dfb0bb31af2a3cab2b7a0e8e310ead89bf069d505909a89d8de4062f8152d77afac8301917c4aa63a701cddcd77dcc1644cdfbd16c522b89acfcfa8677534dd0c989d272eb439720415a83fb73d76c9131d7d25c7962cf4ea78a31ef703e79f9c66e0fae95a5d03e492fdf2b59373705655890a405a1e07b2e52c4ecb2a7be569bcea8bebcdced92aa1f4837ec4ac9db961755b0e5887ea83efbcbd26e34c8da0479be2c4152a5111b4814f/g" bdp-job-client.properties`
#`sed -i '1,3d' bdp-job-client.properties `
#find ./ -name *.zip  -maxdepth 2 | xargs -i rm {}
#find ./ -name *.properties  -maxdepth 2 | xargs -i rm {}

znodeConfig=$(grep 'TssJob.zookeeper.root.path=' bdp-job-client.properties)
sed -i "3c ${znodeConfig}-hive233" bdp-job-client.properties

#sh wtss_deploy.sh [!DEPLOY_PATH]

declare -a NEED_PROJECT_NAMES
NEED_PROJECT_NAMES=("inspect")
#NEED_PROJECT_NAMES=("bdp")
source ${CURRENT_DIR}./wtss_tools/wtss_user.properties
WTSS_USER=${WTSS_USER}
WTSS_PWD=${WTSS_PWD}

for project_name in ${NEED_PROJECT_NAMES[@]};
do
  cp -r ./*.properties ./${project_name}/
  cd ./${project_name}/
  zip -r rrs_aml_${project_name}_wtss.zip *
  cd -
  mv ./${project_name}/rrs_aml_${project_name}_wtss.zip ./

  #环境1 uat
  WTSS_PWD1=`java -cp wtss_tools/tools/Azkaban-AOMP-1.0/lib/rrs-encrypt-1.0.0.jar:wtss_tools/tools/Azkaban-AOMP-1.0/lib/* cn.webank.rrs.encrypt.RrsEncryptUtil ${WTSS_PWD}|xargs java -cp wtss_tools/tools/Azkaban-AOMP-1.0/lib/Azkaban-AOMP-2.0.jar:wtss_tools/tools/Azkaban-AOMP-1.0/lib/* com.webank.azkaban.utils.RSAUtils |awk -F ':' 'NR==2{print $2}'`
  sh wtss_tools/tools/Azkaban-AOMP-1.0/bin/app-ext.sh -f rrs_aml_${project_name}_wtss.zip -p rrs_aml_${project_name}_wtss_ericcheng  -u${WTSS_USER} -w${WTSS_PWD1}
  if [ $? -ne 0 ]; then
    echo "WTSS发版本异常"
    exit 1
  fi

  rm rrs_aml_${project_name}_wtss.zip
done