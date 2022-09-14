-- 客户评价满意度-分析-获取订单状态
SELECT
    CASE
        WHEN order_info_status='unorder' THEN '未下单//unorder'
        WHEN order_info_status='created' THEN '已下单//created'
        WHEN order_info_status='deposited' THEN '已付定金//deposited'
        WHEN order_info_status='paid' THEN '已付款//paid'
        WHEN order_info_status='shipped' THEN '已发货//shipped'
        WHEN order_info_status='succeeded' THEN '已确认收货//succeeded'
    END AS `订单状态`
FROM (
    SELECT
        arrayJoin(['unorder','created','deposited','paid','shipped','succeeded']) AS order_info_status
    FROM numbers(1)
) AS order_info