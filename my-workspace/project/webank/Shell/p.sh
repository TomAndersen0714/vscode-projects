#!/bin/bash
export LC_CTYPE=en_US

addr_20=( M1_20 N_20 M_20 K_20 I_20 F_20 H_20 H_201 D_20 U_20)

M1_20="172.21.98.233"
N_20="172.21.98.234"
M_20="172.21.98.232"
K_20="172.21.42.6"
I_20="172.21.42.7"
F_20="172.21.169.118"
H_20="172.21.67.245"
H_201="172.21.67.220"
D_20="172.21.98.135"
U_20="172.21.102.101"

addr_30=(M1_30 N_30 U1_30 M_30 D_30 F1_30 K_30)
M1_30="172.21.98.236"
N_30="172.21.98.237"
U1_30="172.21.169.119"
M_30="172.21.98.235"
D_30="172.21.98.155"
F1_30="172.21.169.112"
K_30="172.21.169.116"

addr_35=(M1_35 N_35 K_35 I_35 F1_35 H_35 O_35 Q_35 U1_35 M_35 D_35 M12_35 N2_35 D2_35)
M1_35="172.21.98.239"
N_35="172.21.98.240"
K_35="172.21.42.5"
I_35="172.21.42.12"
F1_35="172.21.169.107"
H_35="172.21.35.121"
O_35="172.21.35.123"
Q_35="172.21.42.13"
U1_35="172.21.42.14"
M_35="172.21.98.238"
D_35="172.21.98.156"
M12_35="172.21.99.246"
N2_35="172.21.99.247"
D2_35="172.21.99.245"

addr_40=(M1_40 N_40 D_40 U1_40 U2_40)
M1_40="172.21.100.213"
N_40="172.21.100.214"
D_40="172.21.100.215"
U1_40='172.21.101.149'
U2_40="10.108.130.220"


#JS125="10.107.108.125"
JS110="10.107.108.110"
JS109="10.107.108.109"
JS47="172.21.9.47"
JS36="172.21.9.36"

JS64="10.107.97.64"
JS74="10.107.97.74"
JS91="10.107.97.91"

SPS246="10.107.119.246"
SPS131="172.21.0.131"
SPS184="10.107.118.184"

info_machines(){
  clr_b="\033[1;34m"
  clr_e="\033[0m"
  printf "
=======================================================
   <<< 2.0 DEPOSIT-QS >>>
  =======================
`num=200;for i in ${addr_20[*]};do num=$(expr $num + 1);array=(${i/app/_/ });env=${array[0]};eval new=$(echo '$'"$i");printf "  %s%s%s.[%s: %s]\n"  $clr_b $num $clr_e $env $new;done`
                           
  <<< 3.0 DP-QS >>>
  =======================
`num=300;for i in ${addr_30[*]};do num=$(expr $num + 1);array=(${i/app/_/ });env=${array[0]};eval new=$(echo '$'"$i");printf "  %s%s%s.[%s: %s]\n"  $clr_b $num $clr_e $env $new;done`  

  <<< 3.5 EDP-QS  >>>
  =======================
`num=350;for i in ${addr_35[*]};do num=$(expr $num + 1);array=(${i/app/_/ });env=${array[0]};eval new=$(echo '$'"$i");printf "  %s%s%s.[%s: %s]\n"  $clr_b $num $clr_e $env $new;done`	  

  <<< 4.0 GDP-QS  >>>
  =======================
`num=400;for i in ${addr_40[*]};do num=$(expr $num + 1);array=(${i/app/_/ });env=${array[0]};eval new=$(echo '$'"$i");printf "  %s%s%s.[%s: %s]\n"  $clr_b $num $clr_e $env $new;done`
 
  ========= JOBSERVER UAT ==============
  $clr_b"2"$clr_e. [JS110: $JS110]   -hduser02
   $clr_b"21"$clr_e. [JS110: $JS110] -hduser0206
   $clr_b"22"$clr_e. [JS110: $JS110] -hduser0213
   $clr_b"23"$clr_e. [JS110: $JS110] -hduser0217
  $clr_b"3"$clr_e. [JS109: $JS109]   -hduser02
   $clr_b"31"$clr_e. [JS109: $JS109] -hduser0206
   $clr_b"32"$clr_e. [JS109: $JS109] -hduser0213
   $clr_b"33"$clr_e. [JS109: $JS109] -hduser0217
  $clr_b"8"$clr_e. [JS47: $JS47]   -hduser02
   $clr_b"81"$clr_e. [JS47: $JS47] -hduser0206
   $clr_b"82"$clr_e. [JS47: $JS47] -hduser0213
   $clr_b"83"$clr_e. [JS47: $JS47] -hduser0217 
  $clr_b"9"$clr_e. [JS36: $JS36]   -hduser02
   $clr_b"91"$clr_e. [JS36: $JS36] -hduser0206
   $clr_b"92"$clr_e. [JS36: $JS36] -hduser0213
   $clr_b"93"$clr_e. [JS36: $JS36] -hduser0217
 ========= JOBSERVER SIT ==============
  $clr_b"4"$clr_e. [JS64: $JS64]   -hduser02
   $clr_b"41"$clr_e. [JS64: $JS64] -hduser0206
   $clr_b"42"$clr_e. [JS64: $JS64] -hduser0213
   $clr_b"43"$clr_e. [JS64: $JS64] -hduser0217
  $clr_b"5"$clr_e. [JS74: $JS74]   -hduser02
   $clr_b"51"$clr_e. [JS74: $JS74] -hduser0206
   $clr_b"52"$clr_e. [JS74: $JS74] -hduser0213
   $clr_b"53"$clr_e. [JS74: $JS74] -hduser0217
  $clr_b"6"$clr_e. [JS91: $JS91]   -hduser02
   $clr_b"61"$clr_e. [JS91: $JS91] -hduser0206
   $clr_b"62"$clr_e. [JS91: $JS91] -hduser0213
   $clr_b"63"$clr_e. [JS91: $JS91] -hduser0217

 ========= SPARK STREAMING =========
  $clr_b"71"$clr_e. [SPS246: $SPS246]   -hduser02
  $clr_b"72"$clr_e. [SPS131: $SPS131]   -hduser02
  $clr_b"73"$clr_e. [SPS184: $SPS184]   -hduser02

  q. quit
=======================================================
  "
}

common_cmd(){
  ssh 10.107.101.204
  sudo su - app

}


while :
do 
  info_machines
  read -p "请输入环境序号:" cho
  case $cho in
    201)
      expect /data/app20221129/app/bin/login_v1.ex root $M1_20 0 "less /logs/*online_default.log"
     
    ;;
    202)   
      expect /data/app20221129/app/bin/login_v1.ex root $N_20 0 "less /logs/*online_default.log"
     
    ;;
    203)   
     expect /data/app20221129/app/bin/login_v1.ex root $M_20 0 "less /logs/*online_default.log"
     
    ;;
    204)   
      expect /data/app20221129/app/bin/login_v1.ex root $K_20 0 "less /logs/*online_default.log"
     
    ;;   
    205)
     expect /data/app20221129/app/bin/login_v1.ex root $I_20 0 "less /logs/*online_default.log"
    ;;
    206)
      expect /data/app20221129/app/bin/login_v1.ex root $F_20 0 "less /logs/*online_default.log"
    ;;
    207)
      expect /data/app20221129/app/bin/login_v1.ex root $H_20 0 "less /logs/*online_default.log"
    ;;
    208)
      expect /data/app20221129/app/bin/login_v1.ex root $H_201 0 "less /logs/*online_default.log"
    ;;
    209)
      expect /data/app20221129/app/bin/login_v1.ex root $D_20 0 "less /logs/*online_default.log"
    ;;
    210)
      expect /data/app20221129/app/bin/login_v1.ex root $U_20 0 "less /logs/*online_default.log"
    ;;
	
	301)
      expect /data/app20221129/app/bin/login_v1.ex root $M1_30 0 "less /logs/*online_default.log"
     
    ;;
    3001)
      expect /data/app20221129/app/bin/login_v1.ex root 172.21.104.117 0  "less /logs/*online_default.log"

    ;;
    302)   
      expect /data/app20221129/app/bin/login_v1.ex root $N_30 0 "less /logs/*online_default.log"
     
    ;;
    303)   
     expect /data/app20221129/app/bin/login_v1.ex root $U1_30 0 "less /logs/*online_default.log"
     
    ;;
     3003)
      expect /data/app20221129/app/bin/login_v1.ex root 172.21.102.137 0  "less /logs/*online_default.log"

    ;;

    304)   
      expect /data/app20221129/app/bin/login_v1.ex root $M_30 0 "less /logs/*online_default.log"
     
    ;;   
    305)
     expect /data/app20221129/app/bin/login_v1.ex root $D_30 0 "less /logs/*online_default.log"
    ;;
    306)
      expect /data/app20221129/app/bin/login_v1.ex root $F1_30 0 "less /logs/*online_default.log"
    ;;
	
	307)
      expect /data/app20221129/app/bin/login_v1.ex root $K_30 0 "less /logs/*online_default.log"
    ;;
	
	
	
	351)
      expect /data/app20221129/app/bin/login_v1.ex root $M1_35 0 "less /logs/*online_default.log"
    ;;
	352)
      expect /data/app20221129/app/bin/login_v1.ex root $N_35 0 "less /logs/*online_default.log"                                                
    ;;                                                                                                                           
	353)                                                                                                                         
      expect /data/app20221129/app/bin/login_v1.ex root $K_35 0 "less /logs/*online_default.log"                                                
    ;;                                                                                                                           
	354)                                                                                                                         
      expect /data/app20221129/app/bin/login_v1.ex root $I_35 0 "less /logs/*online_default.log"                                                
    ;;                                                                                                                           
	355)                                                                                                                         
      expect /data/app20221129/app/bin/login_v1.ex root $F1_35 0 "less /logs/*online_default.log"                                               
    ;;                                                                                                                           
	356)
      expect /data/app20221129/app/bin/login_v1.ex root $H_35 0 "less /logs/*online_default.log"
    ;;
	357)
      expect /data/app20221129/app/bin/login_v1.ex root $O_35 0 "less /logs/*online_default.log"
    ;;
	358)
      expect /data/app20221129/app/bin/login_v1.ex root $Q_35 0 "less /logs/*online_default.log"
    ;;
	359)
      expect /data/app20221129/app/bin/login_v1.ex root $U1_35 0 "less /logs/*online_default.log"
    ;;
        360)
      expect /data/app20221129/app/bin/login_v1.ex root $M_35 0 "less /logs/*online_default.log"
    ;;
        361)
      expect /data/app20221129/app/bin/login_v1.ex root $D_35 0 "less /logs/*online_default.log"
    ;;
	362)
      expect /data/app20221129/app/bin/login_v1.ex root $M12_35 0 "less /logs/*online_default.log"
    ;;
	363)
      expect /data/app20221129/app/bin/login_v1.ex root $N2_35 0 "less /logs/*online_default.log"
    ;;
	364)
      expect /data/app20221129/app/bin/login_v1.ex root $D2_35 0 "less /logs/*online_default.log"
    ;;
        401)
      expect /data/app20221129/app/bin/login_v1.ex root $M1_40 0 "less /logs/*online_default.log"
    ;;
	402)
      expect /data/app20221129/app/bin/login_v1.ex root $N_40 0 "less /logs/*online_default.log"
    ;;
	403)
      expect /data/app20221129/app/bin/login_v1.ex root $D_40 0 "less /logs/*online_default.log"
    ;;
        404)
      expect /data/app20221129/app/bin/login_v1.ex root $U1_40 0 "less /logs/*online_default.log"
    ;;
        405)
      expect /data/app20221129/app/bin/login_v1.ex root $U2_40 0 "less /logs/*online_default.log"
    ;;

	
	
    1)
      expect /data/app20221129/app/bin/login.ex  hduser02 $JS125 f62Mu4LA-tbN 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser02/'
    ;;
    11)
      expect /data/app20221129/app/bin/login.ex hduser0206 $JS125 5fYqFs@X5r3H 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0206/'
    ;;
    12)
      expect /data/app20221129/app/bin/login.ex hduser0213 $JS125 ^cLDR8Vun22d 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0213/'
    ;;
    13)
      expect /data/app20221129/app/bin/login.ex hduser0217 $JS125 er7X88-QcMCf 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0217/'
    ;;

    
    2)
      expect /data/app20221129/app/bin/login.ex  hduser02 $JS110 f62Mu4LA-tbN 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser02/'
    ;;
    21)
      expect /data/app20221129/app/bin/login.ex hduser0206 $JS110 5fYqFs@X5r3H 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0206/'
    ;;
    22)
      expect /data/app20221129/app/bin/login.ex hduser0213 $JS110 ^cLDR8Vun22d 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0213/'
    ;;
    23)
      expect /data/app20221129/app/bin/login.ex hduser0217 $JS110 er7X88-QcMCf 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0217/'
    ;;
    
    3)
      expect /data/app20221129/app/bin/login.ex  hduser02 $JS109 f62Mu4LA-tbN 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser02/'
    ;;
    31)
      expect /data/app20221129/app/bin/login.ex hduser0206 $JS109 5fYqFs@X5r3H 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0206/'
    ;;
    32)
      expect /data/app20221129/app/bin/login.ex hduser0213 $JS109 ^cLDR8Vun22d 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0213/'
    ;;
    33)
      expect /data/app20221129/app/bin/login.ex hduser0217 $JS109 er7X88-QcMCf 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0217/'
    ;;

    8)
      expect /data/app20221129/app/bin/login.ex  hduser02 $JS47 f62Mu4LA-tbN 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser02/'
    ;;
    81)
      expect /data/app20221129/app/bin/login.ex hduser0206 $JS47 5fYqFs@X5r3H 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0206/'
    ;;
    82)
      expect /data/app20221129/app/bin/login.ex hduser0213 $JS47 ^cLDR8Vun22d 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0213/'
    ;;
    83)
      expect /data/app20221129/app/bin/login.ex hduser0217 $JS47 er7X88-QcMCf 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0217/'
    ;;

    9)
      expect /data/app20221129/app/bin/login.ex  hduser02 $JS36 f62Mu4LA-tbN 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser02/'
    ;;
    91)
      expect /data/app20221129/app/bin/login.ex hduser0206 $JS36 5fYqFs@X5r3H 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0206/'
    ;;
    92)
      expect /data/app20221129/app/bin/login.ex hduser0213 $JS36 ^cLDR8Vun22d 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0213/'
    ;;
    93)
      expect /data/app20221129/app/bin/login.ex hduser0217 $JS36 er7X88-QcMCf 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0217/'
    ;;


    4)
      expect /data/app20221129/app/bin/login.ex  hduser02 $JS64 f62Mu4LA-tbN 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser02/'
    ;;
    41)
      expect /data/app20221129/app/bin/login.ex hduser0206 $JS64 5fYqFs@X5r3H 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0206/'
    ;;
    42)
      expect /data/app20221129/app/bin/login.ex hduser0213 $JS64 ^cLDR8Vun22d 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0213/'
    ;;
    43)
      expect /data/app20221129/app/bin/login.ex hduser0217 $JS64 er7X88-QcMCf 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0217/'
    ;;

    5)
      expect /data/app20221129/app/bin/login.ex  hduser02 $JS74 f62Mu4LA-tbN 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser02/'
    ;;
    51)
      expect /data/app20221129/app/bin/login.ex hduser0206 $JS74 5fYqFs@X5r3H 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0206/'
    ;;
    52)
      expect /data/app20221129/app/bin/login.ex hduser0213 $JS74 ^cLDR8Vun22d 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0213/'
    ;;
    53)
      expect /data/app20221129/app/bin/login.ex hduser0217 $JS74 er7X88-QcMCf 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0217/'
    ;;


    6)
      expect /data/app20221129/app/bin/login.ex  hduser02 $JS91 f62Mu4LA-tbN 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser02/'
    ;;
    61)
      expect /data/app20221129/app/bin/login.ex hduser0206 $JS91 5fYqFs@X5r3H 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0206/'
    ;;
    62)
      expect /data/app20221129/app/bin/login.ex hduser0213 $JS91 ^cLDR8Vun22d 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0213/'
    ;;
    63)
      expect /data/app20221129/app/bin/login.ex hduser0217 $JS91 er7X88-QcMCf 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser0217/'
    ;;
     
    71)
      expect /data/app20221129/app/bin/login.ex  hduser02 $SPS246 f62Mu4LA-tbN 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser02/'
    ;;
    72)
      expect /data/app20221129/app/bin/login.ex  hduser02 $SPS131 hduser02@1234 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser02/'
    ;;
    73)
      expect /data/app20221129/app/bin/login.ex  hduser02 $SPS184 hduser02 'export PATH=$PATH:/data/bdp/bdp_etl_deploy/hduser02/bin;cd /data/bdp/bdp_etl_deploy/hduser02/'
    ;;
     
    q)
      exit 
    ;;
  esac
done


