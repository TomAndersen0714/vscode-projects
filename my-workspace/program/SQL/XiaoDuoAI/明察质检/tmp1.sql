SELECT hostName(),
    getMacro('replica'),
    user,
    query_id,
    query,
    elapsed,
    memory_usage
FROM clusterAllReplicas('cluster_3s_2r', system.processes)
ORDER BY memory_usage DESC
LIMIT 10