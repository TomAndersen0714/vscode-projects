# export
docker exec -i f2cf59364825 clickhouse-client --port=29000 --query="
    SELECT *
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = 20230815
    AND platform = 'tb'
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT seller_nick
        FROM xqc_dim.shop_latest_all
        WHERE xxHash64(seller_nick) % 8 = 0
    )
    FORMAT Avro
" > /tmp/dwd.xdqc_dialog_all_20230815_0.avro

docker exec -i f2cf59364825 clickhouse-client --port=29000 --query="
    SELECT *
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = 20230815
    AND platform = 'tb'
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT seller_nick
        FROM xqc_dim.shop_latest_all
        WHERE xxHash64(seller_nick) % 8 = 1
    )
    FORMAT Avro
" > /tmp/dwd.xdqc_dialog_all_20230815_1.avro

docker exec -i f2cf59364825 clickhouse-client --port=29000 --query="
    SELECT *
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = 20230815
    AND platform = 'tb'
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT seller_nick
        FROM xqc_dim.shop_latest_all
        WHERE xxHash64(seller_nick) % 8 = 2
    )
    FORMAT Avro
" > /tmp/dwd.xdqc_dialog_all_20230815_2.avro

docker exec -i f2cf59364825 clickhouse-client --port=29000 --query="
    SELECT *
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = 20230815
    AND platform = 'tb'
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT seller_nick
        FROM xqc_dim.shop_latest_all
        WHERE xxHash64(seller_nick) % 8 = 3
    )
    FORMAT Avro
" > /tmp/dwd.xdqc_dialog_all_20230815_3.avro

docker exec -i f2cf59364825 clickhouse-client --port=29000 --query="
    SELECT *
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = 20230815
    AND platform = 'tb'
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT seller_nick
        FROM xqc_dim.shop_latest_all
        WHERE xxHash64(seller_nick) % 8 = 4
    )
    FORMAT Avro
" > /tmp/dwd.xdqc_dialog_all_20230815_4.avro

docker exec -i f2cf59364825 clickhouse-client --port=29000 --query="
    SELECT *
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = 20230815
    AND platform = 'tb'
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT seller_nick
        FROM xqc_dim.shop_latest_all
        WHERE xxHash64(seller_nick) % 8 = 5
    )
    FORMAT Avro
" > /tmp/dwd.xdqc_dialog_all_20230815_5.avro

docker exec -i f2cf59364825 clickhouse-client --port=29000 --query="
    SELECT *
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = 20230815
    AND platform = 'tb'
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT seller_nick
        FROM xqc_dim.shop_latest_all
        WHERE xxHash64(seller_nick) % 8 = 6
    )
    FORMAT Avro
" > /tmp/dwd.xdqc_dialog_all_20230815_6.avro

docker exec -i f2cf59364825 clickhouse-client --port=29000 --query="
    SELECT *
    FROM dwd.xdqc_dialog_all
    WHERE toYYYYMMDD(begin_time) = 20230815
    AND platform = 'tb'
    AND seller_nick GLOBAL IN (
        SELECT DISTINCT seller_nick
        FROM xqc_dim.shop_latest_all
        WHERE xxHash64(seller_nick) % 8 = 7
    )
    FORMAT Avro
" > /tmp/dwd.xdqc_dialog_all_20230815_7.avro

# Import
docker exec -i f3f548fad48e clickhouse-client --port=29000 --query="
    INSERT INTO
    dwd.xdqc_dialog_all
    FORMAT Avro
" < ods.xdrs_logs_all.avro