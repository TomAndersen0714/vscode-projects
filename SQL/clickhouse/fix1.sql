SELECT 
    sum(consult_uv) AS consult_uv,
    sum(create_uv) AS create_uv,
    sum(pay_uv) AS pay_uv,
    sum(amount) / 100 AS amount,
    sum(order_cnt) AS order_cnt,
    if(pay_uv = 0, 0, amount / pay_uv) AS pct,
    if(consult_uv = 0, 0, pay_uv / consult_uv) AS conversion,
    day
FROM app_mp.conversion_all
WHERE mode = 'SEND'
    AND (shop_id = '%s')
    AND (
        day between %d and %d
    )
GROUP BY (shop_id, day)
ORDER BY day DESC;


	
moto仔
5f029f2df7fa220016a0d9c4


SELECT sum(consult_uv) AS consult_uv,
    sum(create_uv) AS create_uv,
    sum(pay_uv) AS pay_uv,
    sum(amount) / 100 AS amount,
    sum(order_cnt) AS order_cnt,
    if(pay_uv = 0, 0, amount / pay_uv) AS pct,
    if(consult_uv = 0, 0, pay_uv / consult_uv) AS conversion,
    day
FROM app_mp.conversion_all
WHERE mode = 'SEND'
    AND (shop_id = '5f029f2df7fa220016a0d9c4')
    AND (day between 20210420 and 20210430)
GROUP BY (shop_id, day)
ORDER BY day DESC


10.19.0.201 zjk-pulsar1-node001
10.19.0.200 zjk-pulsar1-node002
10.19.0.202 zjk-pulsar1-node003
10.19.0.204 zjk-pulsar1-node004
10.19.0.209 zjk-pulsar2-node001
10.19.0.210 zjk-pulsar2-node002
10.19.0.208 zjk-pulsar2-node003
10.19.0.211 zjk-pulsar2-node004
10.19.0.212 pulsar-cluster02-slb
10.19.0.203 pulsar-cluster01-slb