set -ex

#创建部署目录
mkdir -p [!DEPLOY_PATH]/[!PKG_NAME]

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

ln -nsf [!DEPLOY_PATH]/[!PKG_NAME] [!DEPLOY_PATH]/../aml_bdp

#清除历史部署包,仅在测试环境使用，生产环境请删除
cd [!DEPLOY_PATH]
pkgname=[!PKG_NAME]
prestr=${pkgname:0:21}
find ./ -maxdepth  1 ! -path "./"  -atime +7|grep ${prestr}|xargs -i rm -rf {}