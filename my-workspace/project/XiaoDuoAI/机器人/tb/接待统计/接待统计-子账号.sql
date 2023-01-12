-- Impala
SELECT  shop_id,
        mode,
        platform,
        sub_nick as "子账号",
        sum(received_cuv) as "服务买家人数",
        sum(received_session) as "服务买家人次",
        sum(buyer_session) as "买家发起会话",
        sum(seller_session) as "客服发起会话",
        sum(robot_session) as robot_session,
        sum(received_pv) as "接收问题数",
        sum(identified_pv) as "识别问题数",
        sum(manual_reply_mv)  as  "人工回复数",
        round(sum(reply_rt)/sum(reply_pair)/1000,2) as "账号平均响应时长（秒）",
        round(sum(manual_reply_rt)/sum(manual_reply_pair)/1000,2) as "人工回复平均响应时长（秒）",
        sum(auto_reply_pv) as "机器人自动回复",
        sum(click_reply_pv) as "人工点击采纳",
        round((sum(auto_reply_pv)+sum(click_reply_pv))/sum(received_pv) * 100,2)  as "机器人应答率",
     --   reply_pair,
     --   reply_rt,
     --   question_answer_volume 
      round((sum(auto_reply_pv)+ sum(manual_reply_mv) + sum(click_reply_pv))/sum(received_pv)*100,2) as "答问比"
FROM app_mp.sub_nick_receive 
WHERE day  between '{{ day.start }}' and '{{ day.end }}' 
and shop_id ='{{ shop_id }}' 
-- and split_part(sub_nick,':',2)!='服务助手'  
group by 1,2,3,4
order by sum(received_session) desc