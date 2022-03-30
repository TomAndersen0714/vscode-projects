-- 待修正店铺
Spedoo(93092)  Speedo在线客服(93092)
香港puma(511608)  Puma香港官方網上商店(511608)
puma(86919)  PUMA(86919)
香港哥伦比亚(527532)  Columbia HK Brand Store(527532)
GAP(1178004)  Gap(1178004)
Go Wild HK(86916)  MBO官方商城(86916)
NBA大陆(155503)  NBA官方商城(155503)

-- zjk-bigdata006
-- mysql -h 10.20.2.29 -P 3306 -u root -pmypass
SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'Spedoo(93092)' AND platform = 'open'
UPDATE xinghuan.company_tenant SET tenant_label='Speedo在线客服(93092)' WHERE tenant_label='Spedoo(93092)' AND platform = 'open'
SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'Speedo在线客服(93092)'

SELECT * FROM xinghuan.company_tenant WHERE tenant_label = '香港puma(511608)' AND platform = 'open'
UPDATE xinghuan.company_tenant SET tenant_label='Puma香港官方網上商店(511608)' WHERE tenant_label='香港puma(511608)' AND platform = 'open'
SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'Puma香港官方網上商店(511608)' AND platform = 'open'

SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'puma(86919)' AND platform = 'open'
UPDATE xinghuan.company_tenant SET tenant_label='PUMA(86919)' WHERE tenant_label='puma(86919)'
SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'PUMA(86919)' AND platform = 'open'

SELECT * FROM xinghuan.company_tenant WHERE tenant_label = '香港哥伦比亚(527532)' AND platform = 'open'
UPDATE xinghuan.company_tenant SET tenant_label='Columbia HK Brand Store(527532)' WHERE tenant_label='香港哥伦比亚(527532)'
SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'Columbia HK Brand Store(527532)' AND platform = 'open'

SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'GAP(1178004)' AND platform = 'open'
UPDATE xinghuan.company_tenant SET tenant_label='Gap(1178004)' WHERE tenant_label='GAP(1178004)'
SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'Gap(1178004)' AND platform = 'open'

SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'Go Wild HK(86916)' AND platform = 'open'
UPDATE xinghuan.company_tenant SET tenant_label='MBO官方商城(86916)' WHERE tenant_label='Go Wild HK(86916)'
SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'MBO官方商城(86916)' AND platform = 'open'

SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'NBA大陆(155503)' AND platform = 'open'
UPDATE xinghuan.company_tenant SET tenant_label='NBA官方商城(155503)' WHERE tenant_label='NBA大陆(155503)'
SELECT * FROM xinghuan.company_tenant WHERE tenant_label = 'NBA官方商城(155503)' AND platform = 'open'