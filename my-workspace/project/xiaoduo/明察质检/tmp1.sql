alter table polaris.shop_overview_dd
add if not exists range partition 20230810 <= VALUES < 20230811;

alter table polaris.shop_overview_dd
add if not exists range partition 20230811 <= VALUES < 20230812;

alter table polaris.shop_overview_dd
add if not exists range partition 20230812 <= VALUES < 20230813;

alter table polaris.shop_overview_dd
add if not exists range partition 20230813 <= VALUES < 20230814;

alter table polaris.shop_overview_dd
add if not exists range partition 20230814 <= VALUES < 20230815;

alter table polaris.shop_overview_dd
add if not exists range partition 20230815 <= VALUES < 20230816;


alter table polaris.shop_overview_dd
add if not exists range partition VALUES < 20230501;

alter table polaris.shop_overview_dd
drop range partition VALUES < 20230501;

alter table polaris.shop_overview_dd
drop range partition VALUES < 20230501;

alter table polaris.shop_overview_dd
add if not exists range partition 20230503 <= VALUES < 20230816;


alter table polaris.shop_overview_dd
drop range partition 20230503 <= VALUES < 20230816;