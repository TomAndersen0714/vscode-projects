
select msg_time, if (real_buyer_nick is not null and real_buyer_nick!='',real_buyer_nick,cnick) as cnick, snick, msg, answer_id, act, send_msg_from, is_withdraw from dwd.withdraw_context where query_id='6407ff5b68482000015dc6d9' order by msg_time-- trace:cb85a18162be48160000001679370217


!
select msg_time, if (real_buyer_nick is not null and real_buyer_nick!='',real_buyer_nick,cnick) as cnick, snick, msg, answer_id, act, send_msg_from, is_withdraw from dwd.withdraw_context where query_id='64183f4afd1ac200015aa8a0' order by msg_time-- trace:190fb96b161849710000001679370342


!
Query: describe formatted dwd.withdraw_context
+------------------------------+-----------------------------------------------------------------+----------------------+
| name                         | type                                                            | comment              |
+------------------------------+-----------------------------------------------------------------+----------------------+
| # col_name                   | data_type                                                       | comment              |
|                              | NULL                                                            | NULL                 |
| snick                        | string                                                          | NULL                 |
| cnick                        | string                                                          | NULL                 |
| msg                          | string                                                          | NULL                 |
| act                          | string                                                          | NULL                 |
| send_msg_from                | int                                                             | NULL                 |
| msg_time                     | bigint                                                          | NULL                 |
| query_id                     | string                                                          | NULL                 |
| is_withdraw                  | boolean                                                         | NULL                 |
| answer_id                    | string                                                          | NULL                 |
| real_buyer_nick              | string                                                          | NULL                 |
|                              | NULL                                                            | NULL                 |
| # Partition Information      | NULL                                                            | NULL                 |
| # col_name                   | data_type                                                       | comment              |
|                              | NULL                                                            | NULL                 |
| year                         | int                                                             | NULL                 |
| month                        | int                                                             | NULL                 |
| day                          | int                                                             | NULL                 |
|                              | NULL                                                            | NULL                 |
| # Detailed Table Information | NULL                                                            | NULL                 |
| Database:                    | dwd                                                             | NULL                 |
| OwnerType:                   | USER                                                            | NULL                 |
| Owner:                       | root                                                            | NULL                 |
| CreateTime:                  | Tue Nov 10 19:02:31 CST 2020                                    | NULL                 |
| LastAccessTime:              | UNKNOWN                                                         | NULL                 |
| Retention:                   | 0                                                               | NULL                 |
| Location:                    | hdfs://nameservice1/user/hive/warehouse/dwd.db/withdraw_context | NULL                 |
| Table Type:                  | MANAGED_TABLE                                                   | NULL                 |
| Table Parameters:            | NULL                                                            | NULL                 |
|                              | DO_NOT_UPDATE_STATS                                             | true                 |
|                              | transient_lastDdlTime                                           | 1661159138           |
|                              | NULL                                                            | NULL                 |
| # Storage Information        | NULL                                                            | NULL                 |
| SerDe Library:               | org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe     | NULL                 |
| InputFormat:                 | org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat   | NULL                 |
| OutputFormat:                | org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat  | NULL                 |
| Compressed:                  | No                                                              | NULL                 |
| Num Buckets:                 | 0                                                               | NULL                 |
| Bucket Columns:              | []                                                              | NULL                 |
| Sort Columns:                | []                                                              | NULL                 |
+------------------------------+-----------------------------------------------------------------+----------------------+

!
+------------------------------------------------------------------------------------+
| Explain String                                                                     |
+------------------------------------------------------------------------------------+
| Max Per-Host Resource Reservation: Memory=36.00MB Threads=3                        |
| Per-Host Resource Estimates: Memory=172MB                                          |
| WARNING: The following tables are missing relevant table and/or column statistics. |
| dwd.withdraw_context                                                               |
|                                                                                    |
| PLAN-ROOT SINK                                                                     |
| |                                                                                  |
| 02:MERGING-EXCHANGE [UNPARTITIONED]                                                |
| |  order by: msg_time ASC                                                          |
| |                                                                                  |
| 01:SORT                                                                            |
| |  order by: msg_time ASC                                                          |
| |  row-size=79B cardinality=unavailable                                            |
| |                                                                                  |
| 00:SCAN HDFS [dwd.withdraw_context]                                                |
|    partitions=848/863 files=8298 size=29.07GB                                      |
|    predicates: query_id = '64183f4afd1ac200015aa8a0'                               |
|    row-size=90B cardinality=unavailable                                            |
+------------------------------------------------------------------------------------+

