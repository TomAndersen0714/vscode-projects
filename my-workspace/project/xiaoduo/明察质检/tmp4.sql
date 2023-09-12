INSERT INTO dwd.xdqc_dialog_all
SELECT *
FROM remote('10.20.133.175:9000', 'dwd.xdqc_dialog_all')
WHERE toYYYYMMDD(begin_time) = 20230707
AND platform = 'ks'

INSERT INTO dwd.xdqc_dialog_all
SELECT *
FROM remote('10.20.133.174:9000', 'dwd.xdqc_dialog_all')
WHERE toYYYYMMDD(begin_time) = 20230707
AND platform = 'dy'