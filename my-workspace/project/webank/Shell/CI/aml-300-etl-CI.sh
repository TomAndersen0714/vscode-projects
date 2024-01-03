#自动获取本分支号
git_version_no=${GIT_BRANCH/"origin/"/}

#判断版本号是否相等，不等则直接退出
if [  "${PRODUCT_VERSION}-stable" != "$git_version_no" ]; then
    echo " 打包分支号不对!"
     exit 1
fi

sh -x build_300_all.sh pace
tar_file=`ls ${WORKSPACE}/target/*.tar.gz`
aomp-upload ${tar_file}
exit $?