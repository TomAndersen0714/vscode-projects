aml_bdp.bin.utils.pythonUtil.etlTool.main:
    # 解析命令行参数
    args = parser.parse_args()
    # 基于命令行参数, 构建对应的内置 DateTime 变量
    dataDate = DataDate(args.date, args.freq).dataDate

    # 根据 type 执行对应类型的 Task
    aml_bdp.bin.utils.pythonUtil.etlTool.excuteTask

        # 执行指定SQL, 基于执行结果生成报告, 并发送
        elif 'monitor' == args.type:
            sendMonitorMessage(args.date, args.hqlPath, freq=args.freq)
                aml_bdp.bin.utils.pythonUtil.commonMonitor.sendMonitorMessage
                    # 基于 arg.date 生成内置日期参数
                    dataDate = DataDate(dataDate).dataDate
                    # 创建 Message 实例
                    msg = Message()
                        aml_bdp.bin.utils.pythonUtil.MessageUtil.Message.__init__
                            # 加载 {currDir}/../../../conf 路径下的 .properties 文件
                            self.conf = properties
                    # 使用内置日期参数, 更新 msg.conf
                    msg.conf.update(dataDate)

                    # 读取 sql 文件, 并使用 msg.conf 来 format {} 变量表达式
                    sqlContent = getSqlContent(monitorSqlFile, msg.conf, dataDate)
                    # 生成并执行 SQL 命令, 保留查询请求状态和结果
                    cmd = f'''hive -S -e "{sqlContent}"'''
                    status, result = execShellResult(cmd)


    # 执行自动化验证
    aml_bdp.bin.utils.pythonUtil.etlTool.excuteValidate
