{
    "http_endpoint": ":8155",
    "grpc_endpoint": ":8156",
    "pprof_endpoint": ":8157",
    "stream_step_size": 1000,
    "std_log": {
        "level": "info",
        "file": "/var/log/xiaoduo/query-sdk.log",
        "err_file": "/var/log/xiaoduo/query-sdk.err.log",
        "app_name": "query-sdk",
        "auto_init": true
    },
    "api_log": {
        "level": "info",
        "file": "/var/log/xiaoduo/query-sdk.api.log",
        "app_name": "query-sdk",
        "auto_init": true
    },
    "switcher": ["10.20.0.125:2379"],
    "rate_limit": {
        "default": {
            "impala": {
                "qpm": 600,
                "max": 10
            },
            "clickhouse": {
                "qpm": 600,
                "max": 20
            },
            "mysql": {
                "qpm": 600,
                "max": 10
            },
            "postgres":{
                "qpm": 1200,
                "qps": 20,
                "max": 10
            }
        },
        "config-check": {
            "impala": {
                "qpm": 600,
                "max": 10
            },
            "clickhouse": {
                "qpm": 600,
                "max": 20
            },
            "mysql": {
                "qpm": 600,
                "max": 10
            },
            "postgres":{
                "qpm": 1200,
                "qps": 20,
                "max": 10
            }
        },
        "data-visual":{
            "impala": {
                "qpm": 600,
                "max": 10
            },
            "clickhouse": {
                "qpm": 1200,
                "max": 41
            },
            "mysql": {
                "qpm": 600,
                "max": 10
            }
        },
        "csm":{
            "impala": {
                "qpm": 240,
                "max": 4
            },
            "clickhouse": {
                "qpm": 240,
                "max": 4
            },
            "mysql": {
                "qpm": 240,
                "max": 4
            }
        },  
        "snowball":{
            "impala": {
                "qpm": 2,
                "max": 1
            },
            "clickhouse": {
                "qpm": 2,
                "max": 1
            },
            "mysql": {
                "qpm": 100,
                "max": 20
            }
        },      
        "x-data-x-data": {
            "impala": {
                "qpm": 300,
                "max": 5
            },
            "clickhouse": {
                "qpm": 1200,
                "max": 20
            },
            "mysql": {
                "qpm": 600,
                "max": 10
            }
        },
        "x-data-screen": {
            "impala": {
                "qpm": 300,
                "max": 5
            },
            "clickhouse": {
                "qpm": 1200,
                "max": 20
            },
            "mysql": {
                "qpm": 600,
                "max": 10
            }
        },
        "x-data-shandian": {
            "impala": {
                "qpm": 300,
                "max": 5
            },
            "clickhouse": {
                "qpm": 600,
                "max": 10
            },
            "mysql": {
                "qpm": 600,
                "max": 10
            }
        },
        "x-data-xcm": {
            "impala": {
                "qpm": 300,
                "max": 5
            },
            "clickhouse": {
                "qpm": 1200,
                "max": 20
            },
            "mysql": {
                "qpm": 600,
                "max": 10
            }
        },
        "x-data-xz": {
            "impala": {
                "qpm": 300,
                "max": 5
            },
            "clickhouse": {
                "qpm": 1200,
                "max": 20
            },
            "mysql": {
                "qpm": 600,
                "max": 10
            }
        },
        "x-data-jd": {
            "impala": {
                "qpm": 300,
                "max": 5
            },
            "clickhouse": {
                "qpm": 1800,
                "max": 30
            }
        },
        "x-data-csm": {
            "impala": {
                "qpm": 300,
                "max": 5
            },
            "clickhouse": {
                "qpm": 1200,
                "max": 20
            },
            "mysql": {
                "qpm": 600,
                "max": 10
            }
        },
        "x-data-x-data-trans": {
            "impala": {
                "qpm": 300,
                "max": 5
            },
            "clickhouse": {
                "qpm": 1200,
                "max": 20
            },
            "mysql": {
                "qpm": 600,
                "max": 10
            }
        },
        "x-data-xqc": {
            "impala": {
                "qpm": 600,
                "max": 10
            },
            "clickhouse": {
                "qps": 900,
                "max": 15
            }
        },
        "keban-group": {
            "impala": {
                "qpm": 10,
                "max": 3
            },
            "clickhouse": {
                "qpm": 10,
                "max": 10
            }
        }
    },
    "storage": {
        "ping": "20s",
        "impala": {
            "default": ["10.20.2.28:21050","10.20.2.29:21050","10.20.2.30:21050","10.20.133.176:21050","10.20.133.149:21050"],
            "bee": ["10.20.2.28:21000","10.20.2.29:21000","10.20.2.30:21000","10.20.133.176:21000","10.20.133.149:21000"]
        },
        "clickhouse": {
            "default": ["tcp://10.20.2.28:19000?check_connection_liveness=true&read_timeout=300&alt_hosts=10.20.133.149:19000&max_memory_usage=20000000000","tcp://10.20.133.149:19000?read_timeout=300&alt_hosts=10.20.2.28:19000&max_memory_usage=20000000000"],
            "mp": ["tcp://10.20.2.28:19000?check_connection_liveness=true&read_timeout=300&alt_hosts=10.20.133.149:19000&max_memory_usage=20000000000","tcp://10.20.133.149:19000?read_timeout=300&alt_hosts=10.20.2.28:19000&max_memory_usage=20000000000"],
            "keban": ["tcp://10.20.133.173:19000?debug=false"]
        },
        "mysql" : {
            "default": ["select.user:hT0KABSS*X@tcp(172.16.124.5:3306)/jira?charset=utf8&parseTime=true"],
            "snowball": ["root:Mysql1234!@tcp(10.20.0.173:3306)/snowball?charset=utf8&parseTime=true"]
        },
        "postgres": {
            "default": [
                "host=10.20.133.149 port=5432 user=postgres dbname=mayfly password=mysecretpassword sslmode=disable TimeZone=Asia/Shanghai"
            ]
        }
    }
}