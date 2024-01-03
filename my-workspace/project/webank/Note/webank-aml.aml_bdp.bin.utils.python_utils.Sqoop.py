aml_bdp.bin.utils.python_utils.AmlContext.py
    aml_bdp/bin/utils/python_utils/Sqoop.py:187
        # 去除 python 命令行首个参数, 即当前文件名
        newArgs = list(sys.argv[1:])
        # 强制增加参数 "--loadMysql"
        newArgs.append("--loadMysql")
        main(newArgs)

            aml_bdp.bin.utils.python_utils.Sqoop.main
                # 初始化 amlContext
                amlContext = AmlContext.instance(args, loadMysql=True)
                # 
                if amlContext.hasConf('import_hcatalog'):
                    importHcatalogPartitionTable(amlContext)
                        aml_bdp.bin.utils.python_utils.Sqoop.importHcatalogPartitionTable:
                            #
                            ...
                            # 使用 AmlContext.properties 和 env环境变量 转换 shell-style 变量
                            oriCmd = amlContext.convertEnvVariable(oriCmd)
                            # 解析 sqoop 命令行参数, AmlContext中解析的是 python 命令行参数
                            commandLines = CommandLineArgs.parse(re.split('\s+', oriCmd))

                            # 删除分区
                            if amlContext.hasConf('drop-partition'):
                                truncateIfExistsHivePartition(commandLines)
                            
                            # 删除表
                            elif amlContext.hasConf('truncate-table'):
                                truncateTable(commandLines)
                            
                            # 格式化 sqoop 命令行
                            oriCmd = commandLines.convertSpecialWithSpace()
                            cmds = f"sqoop import " + """ -D mapreduce.job.queuename=${your_queue_name} --connect "${url}?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" --username ${username} --password ${mysql_password} --hive-overwrite --fields-terminated-by '|'  -null-string '\\\\N' -null-non-string '\\\\N' --hcatalog-database ${DATABASE_NAME}  --hcatalog-storage-stanza 'stored as orc tblproperties ("orc.compress"="SNAPPY")' -m 1 """ + f' {oriCmd}'

                            # 替换 shell 命令中的变量
                            finalCmd = amlContext.convertEnvVariable(cmds)
                            # 执行 shell 命令
                            commonUtil.execShell(finalCmd)

                elif amlContext.hasConf('import_txt'):
                    importTxt(amlContext)
                # 
                elif amlContext.hasConf('import_txt_by_log'):
                    importTxtByHcatalog(amlContext)
                # 
                elif amlContext.hasConf('export_hcatalog'):
                    exportHcatalog(amlContext)
                # 
                elif amlContext.hasConf('export_txt'):
                    exportTxt(amlContext)