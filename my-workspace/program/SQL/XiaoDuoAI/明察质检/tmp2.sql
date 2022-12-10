SELECT id AS alert_id,
    alert_local.id,
    alert_local.platform,
    alert_local.level,
    alert_local.warning_type,
    alert_local.dialog_id,
    alert_local.message_id,
    alert_local.time,
    alert_local.is_finished,
    alert_local.finish_time,
    alert_local.shop_id,
    alert_local.snick,
    alert_local.cnick,
    alert_local.employee_name,
    alert_local.superior_name
FROM xqc_ods.alert_local FINAL
WHERE (
        (day >= 20220909)
        AND (day <= 20221207)
    )
    AND (
        (
            shop_id IN ['61d6a38716bbc36cb34dfd4c', '61d6a38716bbc36cb34dfd56', '61d6a38716bbc36cb34dfd52', '61c193262ba76f001d769b90', '5f3cd79bb7fba70017c854bb', '61de9efadba7c00020cfd5f5', '61dd56c1df229d00176cdce8', '6170ddb2abefdb000c773b0a', '616d2b651ffab50014d6f922', '6172894009841b000fafffc9', '61ee6acf09f2f12c5f2f1852', '61ee6acf09f2f12c5f2f182a', '61ee6acf09f2f12c5f2f1839', '616d49b11ffab50016d6fa49', '616fccff269ebf000e1b88b0', '616e207da08ae900109dcf33', '616e1b70abefdb0010773a23', '616d282d1ffab50012d6f485', '61d6a38716bbc36cb34dfd48', '61d6a38716bbc36cb34dfd58', '61ee6acf0