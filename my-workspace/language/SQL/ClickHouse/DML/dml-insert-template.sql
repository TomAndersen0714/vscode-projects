INSERT INTO xqc_dim.goods_all VALUES
(
    generateUUIDv4(), toUInt64(now()), toUInt64(now()), '63563e97c5bdd4d1fdfc2282', '5e7dbfa6e4f3320016e9b7d1', 'jd', '测试商品1', '10065461796215', 10.1, 1, ['新品'], toUInt64(now())
),
(
    generateUUIDv4(), toUInt64(now()), toUInt64(now()), '63563e97c5bdd4d1fdfc2282', '5e7dbfa6e4f3320016e9b7d1', 'jd', '测试商品2', '10066530366721', 9999, 1, ['高单价'], toUInt64(now())
),
(
    generateUUIDv4(), toUInt64(now()), toUInt64(now()), '63563e97c5bdd4d1fdfc2282', '5e7dbfa6e4f3320016e9b7d1', 'jd', '测试商品3', '10065461796217', 10.1, 1, [], toUInt64(now())
)