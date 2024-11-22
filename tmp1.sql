col_name        data_type       comment
data_dt                 string                  数据日期：YYYY-MM-DD     
cust_id                 string                  客户号：主键              
cust_name               string                  客户名称                
org_id                  string                  开户机构代号              
gender                  string                  性别                  
birthday                string                  出生日期                
marital_status          string                  婚姻状况                
edu_background          string                  最高学历                
degree                  string                  最高学位                
profession              string                  职业                  
home_tel                string                  住宅电话                
mobile_phone            string                  手机号码                
busi_phone              string                  单位电话                
fax_number              string                  传真号码                
other_number            string                  其他号码                
email                   string                  电子邮箱                
postal_add              string                  通讯地址                
postcode                string                  通讯地址邮政编码            
census_reg_add          string                  户籍地址                
resident_flg            string                  居民标志01居民02非居民       
city_village_flg        string                  农户标志：Y是N否           
related_flg             string                  是否关联方：Y：是N：否        
stock_pct               decimal(20,4)           持股比率：是否关联方=Y：是时填写   
id_type                 string                  证件类型                
id_no                   string                  证件号码                
cust_type               string                  客户类型:01-普通个人02-小微企业主03-个体工商户,默认为空
indu_type               string                  行业类别:当客户类型in(02-小微企业主，03-个体工商户)时，必填。
country_code            string                  国籍或注册地国家代码          
income_family_avg_m     decimal(20,4)           家庭月均收入              
company_name            string                  单位名称                
company_add             string                  单位地址                
company_postcode        string                  单位地址邮政编码            
employed_year           string                  本单位工作起始年份           
duty                    string                  职务                  
title                   string                  职称                  
year_income             decimal(20,4)           年收入                 
wage_acct               string                  工资账号                
wage_acct_bank          string                  工资账户开户银行            
live_add                string                  居住地址                
live_postcode           string                  居住地址邮政编码            
dwelling_condition      string                  居住状况                
spouse_name             string                  配偶姓名                
spouse_id_type          string                  配偶证件类型              
spouse_id_no            string                  配偶证件号码              
spouse_company          string                  配偶工作单位              
spouse_phone            string                  配偶联系电话              
onoff_flg               string                  境内境外标志              
sys_src_cd              string                  源系统代码               
limit_500_flg           string                  是否单户授信500万以下        
new_cust_id             string                  合并后客户号              
is_staff                string                  是否本行员工              
source                  string                  客户来源                
occupation_type         string                  职业类型                
nation                  string                  民族                  
id_expiry_date          string                  身份证件到期日             
licence_issuing         string                  发证机关                
id_add                  string                  身份证地址               
effective_date          string                  证件生效日期              
spouse_telephone        string                  配偶固定电话              
personal_monthly_income string                  个人月收入               
family_monthly_income   string                  家庭月收入               
spouse_ecif_no          string                  配偶对应客户号             
unit_properties         string                  单位性质                
telephone_area_code     string                  手机号所属区域码            
other_add               string                  其他地址                
payroll_credit          string                  代发工资                
personal_annual_income  string                  个人年收入               
family_annual_income    string                  家庭年收入               
occupation_type_new     string                  职业类型                
occupation_type_new_sysid       string                  职业来源sysid           
busi_phone_new          string                  单位电话                
ds                      string                  数据日期                
sysid                   string                  ECIF-DCN            
                 
# Partition Information          
# col_name              data_type               comment             
                 
ds                      string                  数据日期                
sysid                   string                  ECIF-DCN  