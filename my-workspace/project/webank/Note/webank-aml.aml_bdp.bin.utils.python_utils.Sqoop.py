aml_bdp.bin.utils.python_utils.AmlContext.py
    aml_bdp/bin/utils/python_utils/Sqoop.py:187
        # 去除 python 命令行首个参数, 即当前文件名
        newArgs = list(sys.argv[1:])
        # 强制增加参数 "--loadMysql"
        newArgs.append("--loadMysql")
        main(newArgs)
            # 
            aml_bdp.bin.utils.python_utils.Sqoop.main