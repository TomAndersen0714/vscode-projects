"plan"

"== Parsed Logical Plan ==
  'GlobalLimit 10
   +- 'LocalLimit 10
   +- 'Sort ['id DESC NULLS LAST], true
   +- 'Project [*]
   +- 'UnresolvedRelation [mammut_user, aaa_dwd_order_detail_w], [], false

== Analyzed Logical Plan ==
  id: string, member_id: string, order_id: bigint, order_state: bigint, item_id: bigint, item_name: string, item_cnt: bigint, item_total_amount: decimal(26,2), item_pay_amount: decimal(26,2), order_time: string GlobalLimit 10
   +- LocalLimit 10
   +- Sort [id#287 DESC NULLS LAST], true
   +- Project [id#287, member_id#288, order_id#289L, order_state#290L, item_id#291L, item_name#292, item_cnt#293L, item_total_amount#294, item_pay_amount#295, order_time#296]
   +- SubqueryAlias spark_catalog.mammut_user.aaa_dwd_order_detail_w
   +- DataMaskMarker
   +- HiveTableRelation [`mammut_user`.`aaa_dwd_order_detail_w`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#287, member_id#288, order_id#289L, order_state#290L, item_id#291L, item_name#292, item_cnt#29..., Partition Cols: []]

== Optimized Logical Plan ==
  GlobalLimit 10
   +- LocalLimit 10
   +- Sort [id#287 DESC NULLS LAST], true
   +- HiveTableRelation [`mammut_user`.`aaa_dwd_order_detail_w`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#287, member_id#288, order_id#289L, order_state#290L, item_id#291L, item_name#292, item_cnt#29..., Partition Cols: []]

== Physical Plan ==
  TakeOrderedAndProject(limit=10, orderBy=[id#287 DESC NULLS LAST], output=[id#287,member_id#288,order_id#289L,order_state#290L,item_id#291L,item_name#292,item_cnt#293L,item_total_amount#294,item_pay_amount#295,order_time#296])
   +- Scan hive mammut_user.aaa_dwd_order_detail_w [id#287, member_id#288, order_id#289L, order_state#290L, item_id#291L, item_name#292, item_cnt#293L, item_total_amount#294, item_pay_amount#295, order_time#296], HiveTableRelation [`mammut_user`.`aaa_dwd_order_detail_w`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#287, member_id#288, order_id#289L, order_state#290L, item_id#291L, item_name#292, item_cnt#29..., Partition Cols: []] "