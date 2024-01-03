SELECT
    COUNT(1) AS cnt,
    COUNT(DISTINCT cust_id) AS cust_id_ucnt,
    COUNT(isnull(cust_id)) AS cust_id_nvl_cnt
FROM imd_dm_safe.rrs_bdm_icustomer_merger_mid
WHERE ds = '20230914'


SELECT
    COUNT(1) AS cnt,
    COUNT(DISTINCT ecif_no) AS ecif_no_ucnt,
    COUNT(isnull(ecif_no)) AS ecif_no_nvl_cnt
FROM rpd_ecif_ods_safe.ecif_webank_base_info_output
WHERE ds = '20230914'


-- 整体数据量对比
SELECT
    COUNT(1) AS sum_cnt,
    COUNT(isnull(a.cust_id_nvl)) AS b_more_a_cnt,
    COUNT(isnull(b.cust_id_nvl)) AS b_less_a_cnt,
FROM (
    SELECT
        nvl(cust_id, '') AS cust_id_nvl,
        *
    FROM imd_dm_safe.rrs_bdm_icustomer_merger_mid
    WHERE ds = '20230914'
) AS a
LEFT JOIN (
    SELECT
        nvl(ecif_no, '') AS cust_id_nvl,
        *
    FROM rpd_ecif_ods_safe.ecif_webank_base_info_output
    WHERE ds = '20230914'
) AS b
WHERE a.cust_id_nvl = b.cust_id_nvl

-- 字段差异数据量对比
SELECT
    COUNT(a.cust_name != b.personal_name) AS cust_name_diff,
    COUNT(a.id_type != b.personal_identification_type) AS id_type_diff,
    COUNT(a.id_no != b.personal_identification_number) AS id_no_diff,
    COUNT(a.gender != b.gender) AS gender_diff,
    COUNT(a.country_code != b.nationality) AS country_code_diff,
    COUNT(a.occupation_type != b.occupation_type) AS occupation_type_diff,
    COUNT(a.busi_phone != b.telephone_01) AS busi_phone_diff,
    COUNT(a.home_tel != b.telephone_02) AS home_tel_diff,
    COUNT(a.mobile_phone != b.telephone_04) AS home_tel_diff,
    COUNT(a.other_number != b.telephone_99) AS other_number_diff,
    COUNT(a.live_add != b.address_01) AS live_add_diff,
    COUNT(a.company_add != b.address_02) AS company_add_diff,
    COUNT(a.census_reg_add != b.address_03) AS census_reg_add_diff,
    COUNT(a.postal_add != b.address_10) AS postal_add_diff,
    COUNT(a.other_add != b.address_99) AS other_add_diff,
    COUNT(a.id_add != b.address_11) AS id_add_diff
FROM (
    SELECT
        nvl(cust_id, '') AS cust_id_nvl,
        *
    FROM imd_dm_safe.rrs_bdm_icustomer_merger_mid
    WHERE ds = '20230914'
) AS a
JOIN (
    SELECT
        nvl(ecif_no, '') AS cust_id_nvl,
        *
    FROM rpd_ecif_ods_safe.ecif_webank_base_info_output
    WHERE ds = '20230914'
) AS b
WHERE a.cust_id_nvl = b.cust_id_nvl