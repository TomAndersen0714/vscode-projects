-- DROP TABLE xqc_dim.company_tenant ON CLUSTER cluster_3s_2r NO DELAY
CREATE TABLE xqc_dim.company_tenant ON CLUSTER cluster_3s_2r
(
    `company_id` String,
    `company_label` String,
    `tenant_id` String,
    `tenant_label` String,
    `platform` String,
    `platform_label` String
)
ENGINE = MySQL('10.20.2.29:3306', 'xinghuan', 'company_tenant', 'root', 'mypass')


-- DROP TABLE xqc_dim.company_tenant

-- DROP TABLE xqc_dim.company_tenant
CREATE VIEW IF NOT EXISTS xqc_dim.company_tenant
AS
SELECT
    company_id,
    '' AS company_label,
    shop_id AS tenant_id,
    '' AS tenant_label,
    platform,
    '' AS platform_label
FROM xqc_dim.shop_latest_all