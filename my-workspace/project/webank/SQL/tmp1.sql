SELECT
    COUNT(1)
FROM (
    SELECT
        ecif_no,
        personal_name,
        personal_identification_type,
        personal_identification_number,
        gender,
        nationality,
        validate,
        occupation_type,
        telephone_01,
        telephone_02,
        telephone_04,
        telephone_99,
        address_01,
        address_02,
        address_03,
        address_10,
        address_99,
        address_11,
        version
    FROM imd_dm_safe.rrs_bdm_icustomer_merger_mid
    WHERE ds = '20231128'
) AS a
JOIN (
    SELECT
        ecif_no,
        personal_name,
        personal_identification_type,
        personal_identification_number,
        gender,
        nationality,
        validate,
        occupation_type,
        telephone_01,
        telephone_02,
        telephone_04,
        telephone_99,
        address_01,
        address_02,
        address_03,
        address_10,
        address_99,
        address_11,
        version
    FROM rpd_ecif_ods_safe.ecif_webank_base_info_output
    WHERE ds = '20231128'
) AS b
WHERE nvl(a.ecif_no, '') != nvl(b.ecif_no, '')
