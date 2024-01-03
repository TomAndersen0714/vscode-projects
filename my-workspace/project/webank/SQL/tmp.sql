select
    count(*) as cnt
    ,sum(a.valid_acct) as valid_acct
    ,sum(a.invalid_acct) as invalid_acct
    ,count(if(a.acc_name is null,'Y',null)) as acc_name_isnull
    ,count(if(a.cst_sex is null,'Y',null)) as cst_sex_isnull
    ,count(if(a.nation is null,'Y',null)) as nation_isnull
    ,count(if(a.occupation is null,'Y',null) )  as occupation_isnull
    ,count(if(a.address1 is null,'Y',null) )  as address1_isnull
    ,count(if(a.contact1 is null,'Y',null) )  as contact1_isnull
    ,count(if(a.id_type is null,'Y',null) )  as id_type_isnull
    ,count(if(a.id_no is null,'Y',null) )  as id_no_isnull
    ,count(if(a.id_deadline is null,'Y',null) )  as id_deadline_isnull
    ,count(if(a.id_deadline is null and a.attr22='N','Y',null) )  as id_deadline_3_isnull
    ,count(if(a.attr6='Y' and a.id_deadline is not null and a.attr1='N','Y',null) )  as overdue_while_open
    ,count(if(a.attr6='Y' and a.id_deadline is not null and a.attr1='N' and a.attr22='N','Y',null) )  as overdue_while_open_without3
    ,count(if(a.attr6='Y' and a.id_deadline is not null and a.id_deadline<regexp_replace('2023-12-25','-',''),'Y',null) )  as id_overdue
    ,count(if(a.attr6='Y' and a.id_deadline is not null and a.id_deadline<regexp_replace(add_months('2023-12-25',-6),'-',''),'Y',null) )  as id_overdue_6m
    ,count(if(a.attr6='Y' and a.id_deadline is not null and a.id_deadline<regexp_replace(date_sub('2023-12-25',180),'-',''),'Y',null) )  as id_overdue_0930_180
    ,count(if(a.attr6='Y' and a.id_remaining_days<-180 , true,null)) as overdue_gt180days  --证件失效超180天
    ,count(if(a.attr6='N' and a.id_deadline is not null,'Y',null) )  as id_deadline_err
    ,count(if(a.all_miss_lwhc_while_open>0,'Y',null) ) as all_miss_lwhc_while_open_cst
    ,sum(a.all_miss_lwhc_while_open) as all_miss_lwhc_while_open
    ,count(if(a.miss_lwhc_while_open>0,'Y',null) )  as miss_lwhc_while_open_cst
    ,sum(a.miss_lwhc_while_open) as miss_lwhc_while_open
    ,count(if(a.all_miss_lwhc_in3days>0,'Y',null) ) as all_miss_lwhc_in3days_cst
    ,sum(a.all_miss_lwhc_in3days) as all_miss_lwhc_in3days
    ,count(if(a.miss_lwhc_in3days>0,'Y',null) )  as miss_lwhc_in3days_cst
    ,sum(a.miss_lwhc_in3days) as miss_lwhc_in3days
    ,count(if(a.company is null,'Y',null) )  as company_isnull
    ,count(if(a.income is null,'Y',null) )  as income_isnull
    ,count(if(a.attr2='Y','Y',null) )  as 9items
    ,count(if(a.attr3='Y','Y',null) )  as 9items_miss_occupation
    ,count(if(a.attr21='N','Y',null) )  as sex_err
    ,count(if(a.address1 is not null and length(a.address1)<5,'Y',null) )  as short_addr
    ,count(if(a.attr23='N',true,null))      as custname_diff_accname  -- 客户名与账户名不同
    ,count(if(a.dup_ecif is not null,true,null))    as dup_ecif    -- 同证件不同客户对应客户号
    ,count(if(a.attr25 is not null,true,null))      as same_tel  -- 同一个手机号有多个客户使用
from rrs_aml_tb_cst_pers_check_details a
inner join rrs_aml_300_cust_ready b
    on a.cst_no=b.cst_no
    and b.ds='2021-06-30'
    and b.scope='120W'
where a.ds='2023-12-25'