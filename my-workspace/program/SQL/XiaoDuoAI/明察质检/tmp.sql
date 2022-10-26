10.20.0.140 zjk-bigdata002
10.20.2.28 zjk-bigdata006
10.20.2.29 zjk-bigdata007
10.20.2.30 zjk-bigdata005
10.20.2.129 jstzjk-002129-prod-tb-bigdata-bigdata
10.20.133.149 zjk-bigdata008
10.20.133.176 jstzjk-133176-prod-tb-bigdata-bigdata
10.20.133.177 jstzjk-133177-prod-tb-bigdata-cdh


sshd:10.20.0.140:allow
sshd:10.20.2.28:allow
sshd:10.20.2.29:allow
sshd:10.20.2.30:allow
sshd:10.20.2.129:allow
sshd:10.20.133.149:allow
sshd:10.20.133.176:allow
sshd:10.20.133.177:allow


curl 'http://znzjk-134218-test-mini-bigdata-clickhouse:7180/cmf/add-hosts-wizard/wizard' \
  -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' \
  -H 'Accept-Language: zh-CN,zh;q=0.9,en;q=0.8' \
  -H 'Connection: keep-alive' \
  -H 'Cookie: __utmz=194333631.1646967665.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); gr_user_id=197d10f3-a020-4fd3-a029-9a5091bf9f18; sensorsdata2015jssdkcross=%7B%22%24device_id%22%3A%2217f9b001fc74f5-099203cae7592-576153c-1327104-17f9b001fc8a32%22%7D; Hm_lvt_103e9b51f831e7a08a4e57fae4d0fb05=1663229320; 934a89823bed3e05_gr_last_sent_cs1=%E6%96%B0%E6%B5%8B%E8%AF%95; 934a89823bed3e05_gr_cs1=%E6%96%B0%E6%B5%8B%E8%AF%95; __utmc=194333631; CLOUDERA_MANAGER_SESSIONID=node0gep5san0zkey1r7xit4suf2on13394.node0; __utma=194333631.1605594147.1646967665.1666789570.1666792430.57; __utmt=1; __utmb=194333631.35.9.1666793200530' \
  -H 'Referer: http://znzjk-134218-test-mini-bigdata-clickhouse:7180/cmf/add-hosts-wizard/welcome' \
  -H 'Upgrade-Insecure-Requests: 1' \
  -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36' \
  --compressed