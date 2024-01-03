# 参考 ProcessOn 详解

# AmlContext 实例化方法
aml_bdp.bin.utils.python_utils.AmlContext.AmlContext.instance:
    # 通过双检锁的方式来实现单例模式
    if not hasattr(AmlContext, "_instance"):
        with AmlContext._instance_lock:
            if not hasattr(AmlContext, "_instance"):

                # 初始化
                AmlContext._instance = AmlContext(commandLineArgs, dataDate, **kwargs)
                # 
                AmlContext._instance.otherInit()
