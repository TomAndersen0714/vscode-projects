SELECT
   day,
   platform,
   shop_id,
   session_id,
   buyer_nick,
   snick,
   focus_goods_id,
   c_recv_start_time,
   c_recv_end_time,
   s_send_start_time,
   s_send_end_time,
   session_recv_cnt,
   session_send_cnt,
   has_transfer,
   transfer_from_snick,
   transfer_to_snick
FROM ft_ods.xdrs_logs_all