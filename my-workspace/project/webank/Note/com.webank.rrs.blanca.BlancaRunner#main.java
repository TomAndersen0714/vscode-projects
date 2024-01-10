com.webank.rrs.blanca.BlancaRunner#main:
    int exitCode = runApp(args);
        com.webank.rrs.blanca.BlancaRunner#runApp
            EtlContext etlContext = EtlContextFactory.getOrCreateEngineContext(args);
                // 依据AppModule中定义的装载过程, 创建 Injector, 其中 Injector 是对应模块的注入类的管理器 
                Injector injector = Guice.createInjector(new AppModule(args, true));
                    // 创建 AppModule 实例
                    com.webank.rrs.blanca.api.AppModule#AppModule

                    // createInjector 调用 AppModule 模块的 configure 方法
                    com.webank.rrs.blanca.api.AppModule#configure
                        // 部署 PostConstructModule 模块, 主要用于执行 GuicePostConstruct 注解的方法
                        install(new PostConstructModule());
                            com.webank.rrs.blanca.guice.PostConstructModule#configure
                                // 添加监听器, 配置在每次 Inject 注入实例时触发当前(this)的 hear 方法
                                binder.bindListener(Matchers.any(), this);

                        // 部署 Rest 模块
                        install(new RestModule());
                            com.webank.rrs.blanca.api.rest.RestModule#configure
                                binder().bind(RrsEtlRestProducer.class).toProvider(RestProduceProvider.class).in(Scopes.SINGLETON);
                                binder().bind(RestApi.class).toProvider(RestProvider.class).in(Scopes.SINGLETON);
                                // 给 RestApiList.class 绑定 Provider 的实现类 RestProviderList.class
                                binder().bind(RestApiList.class).toProvider(RestProviderList.class).in(Scopes.SINGLETON);
                                    // 每次在 @Inject 注解处, 直接调用 Provider.get 方法, 来生成对应的实例, 并注入给 @Inject 注解的变量
                                    com.webank.rrs.blanca.api.rest.RestProviderList#get
                                        List<RestApi> restApis = new ArrayList<>();
                                        // 加载默认的 RestAPI 请求配置
                                        RestApi defaultApi = restProducer.defaultRestTemplate(fileConfigParser.getConfigMap());
                                            com.webank.rrs.blanca.api.rest.RrsEtlRestProducer#defaultRestTemplate(java.util.Map<java.lang.String,java.lang.String>):
                                                // 读取配置中的 ETL_GATEWAY_URI(gateway.uri) 变量, 目前默认为 DQC
                                                HttpComponentsClientHttpRequestFactory factory = getBaseHttpRequestFactory();
                                                String uri = restProps.get(CommonConstant.ETL_GATEWAY_URI);
                                        restApis.add(defaultApi);
                                        Config config = fileConfigParser.getConfig();
                                        // 如果 fileConfigParser 配置中存在 ETL_WEB_RESOURCE 参数, 则继续获取其他参数, 并构建
                                        // 对应的 HTTP 请求模板对象, 后续会在 webConfBuilder 对象注入过程中, 发起对应的 HTTP 请求
                                        if (config.hasPath(CommonConstant.ETL_WEB_RESOURCE)) {
                                            String webSource = config.getString(CommonConstant.ETL_WEB_RESOURCE);
                                            RestApi extraWebApi = restProducer.restTemplate(webSource, fileConfigParser.getConfig());
                                            restApis.add(extraWebApi);
                                        }


                        // 部署 RrsConfigModule 模块, 即配置信息模块
                        install(new RrsConfigModule(cmds));
                            com.webank.rrs.blanca.config.RrsConfigModule#configure
                                // 解析命令行参数
                                CommandLineArgs commandLineArgs = new SimpleCommandLineArgsParser().parse(cmds);
                                    // 解析并保存 --var=val --var 参数
                                    commandLineArgs.addOptionArg(optionName, optionValue);
                                    // 解析并保存 var 参数
                                    commandLineArgs.addNonOptionArg(arg);
                                
                                // 加载 .properties 文件参数配置, 并注入到对应依赖类的注解变量中, 并存储到loadMap
                                // 在所有 @Named("fileConfBuilder") 注解的变量, 注入 PropertiesConfigBuilder 的一个单例对象
                                binder().bind(ConfigParser.class).annotatedWith(Names.named("fileConfBuilder")).to(PropertiesConfigBuilder.class).in(Scopes.SINGLETON);
                                    // 由于设置了监听器, 每次注入 Inject 时, 都会调用其中的 hear 方法, 进而会调用 @GuicePostConstruct 注解标注的方法, 执行构造后的一系列动作
                                    com.webank.rrs.blanca.config.PropertiesConfigBuilder#load
                                        properMap = getConfigFileArgsMap();
                                            com.webank.rrs.blanca.config.PropertiesConfigBuilder#getConfigFileArgsMap
                                                loadProperties(loadMap, rootPath + File.separatorChar + "conf");
                                                    com.webank.rrs.blanca.config.PropertiesConfigBuilder#loadProperties
                                                        // 筛选 conf 路径下的所有 .properties 文件, 且文件名不包含 log4j
                                                        pathname -> (StrUtil.endWith(pathname.getPath(), ".properties", true) && !StrUtil.contains(pathname.getName(), "log4j"))
                                                        // 将每个文件的配置写入到 loadMap 中
                                                        loadMap.put(fileMap.getType(), fileMap);
                                                    // 返回 loadMap
                                                    return loadMap
                                        // 将解析的参数添加到 allConfigs
                                        allConfigs.putAll(entry.getValue().getConfigMap());

                                // 请求所有 RestAPI (默认是DQC, 可通过 ETL_WEB_RESOURCE(web.source.from) 参数支持其他的 RestAPI ), 解析对应的参数
                                // 并注入到对应依赖类的注解变量中, 并存储到 rrsConfMapMap, 然后注入到依赖变量, PS: 注意请求位置和旧版本不同
                                binder().bind(ConfigParser.class).annotatedWith(Names.named("webConfBuilder")).to(WebConfigBuilder.class).in(Scopes.SINGLETON);
                                // 加载环境变量参数配置, 加载 Java properties 配置, 并合并到 rrsConfMap, 然后注入到依赖变量
                                binder().bind(ConfigParser.class).annotatedWith(Names.named("sysConfBuilder")).to(SystemConfigBuilder.class).in(Scopes.SINGLETON);
                                // 加载命令行参数配置, 然后注入到依赖变量
                                binder().bind(ConfigParser.class).annotatedWith(Names.named("cmdConfBuilder")).to(CmdConfigBuilder.class).in(Scopes.SINGLETON);
                                // 复制命令行参数配置, 清除命令行参数中的特定参数, 如 job, type, hqlPath, hqlEngine, batch_date等, 然后注入到依赖变量
                                binder().bind(ConfigParser.class).annotatedWith(Names.named("cmdConfFilterBuilder")).to(CmdConfigFilteredKeyBuilder.class).in(Scopes.SINGLETON);


                                // 在所有 @Named("allConfBuilder") 注解的变量, 注入 BlancaConfigBuilder 的一个单例对象
                                binder().bind(ConfigParser.class).annotatedWith(Names.named("allConfBuilder")).to(BlancaConfigBuilder.class).in(Scopes.SINGLETON);
                                    // 由于设置了监听方法, 其中会调用 @GuicePostConstruct 注解标注的方法
                                    com.webank.rrs.blanca.config.BlancaConfigBuilder#load
                                        // 合并 properties 文件配置, Web 配置, 命令行配置, 生成 typeKeyWeb
                                        Map<String, RrsConfMap> typeKeyWeb = mergePropsAndWeb(propFromFiles, webMap, commandMap);
                                        // 合并命令行配置 commandMap, typeKeyWeb, 生成 finalConfMap
                                        RrsConfMap finalConfMap = mergedConf(commandMap, typeKeyWeb, systemConf).resolve();

                                        // 检测配置是否冲突, 如果冲突, 则打印 WARNING 日志
                                        validateConflict(loaderMap);



                // 获取加载完成的 etlContext
                etlContext = injector.getInstance(RrsEtlContext.class);
            

            // 根据输入的 type 参数, 获取对应的 Engine 类
            ExecuteEngine engine = EngineProducer.getEngine(etlContext);

            // 解密接口依赖引擎各自实现 所以需要在生成引擎之后加载进上下文
            int exitCode = engine.execute(etlContext);
                com.webank.rrs.blanca.factory.engine.HiveHqlEngine#execute
                    // 读取 sqlHeader.hql 文件, 加载 hive SQL 运行时配置
                    String sqlHeadler = getSqlHeadler(etlContext);
                    // 
                    String sql = etlContext.variableCovert(etlContext.readFileString(hqlPath));
                        com.webank.rrs.blanca.api.RrsEtlContext#variableCovert(java.lang.String)
                            return lazyGetConvertChan().convert(cmd);
                                com.webank.rrs.blanca.api.RrsEtlContext#lazyGetConvertChan
                                    convertChain
                                    // 转换 ${} 变量
                                    .registerConverter(new DefaultVariableReplaceConverter(this.configMap,RRsStringSubstitutor.$_ESCAPE_CHAR,false))
                                    // 转换 .*partition_.* 变量
                                    .registerConverter(new MaxPartitionConverter(this))
                                    // 转换内置 DateTime 变量
                                    .registerConverter(new DateConverter(batchDate))
                                    // 转换 #{} 变量
                                    .registerConverter(new DefaultVariableReplaceConverter(this.configMap, RRsStringSubstitutor.WELL_ESCAPE_CHAR, false))
                                    // 转换 ${} 变量, 如果仍然存在无法解析的变量，默认直接报错
                                    .registerConverter(new DefaultVariableReplaceConverter(this.configMap,RRsStringSubstitutor.$_ESCAPE_CHAR,true))
                                    // 转换 $RUNTIME{ 变量
                                    .registerConverter(new RuntimeVariableReplaceConverter(this.configMap));