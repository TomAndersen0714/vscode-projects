docker cp 7a6838cfcc45:/etc/hosts ./7a6838cfcc45_hosts
docker exec -it --user root 7a6838cfcc45 bash -c "echo '10.22.133.216 znzjk-133216-prod-mini-bigdata-bigdata' >> /etc/hosts"



wget znzjk-113174-prod-mini-bigdata-bigdata:19000
wget znzjk-113175-prod-mini-bigdata-bigdata:19000
wget znzjk-133216-prod-mini-bigdata-bigdata:19000
wget v1mini-bigdata-002:19000
wget v1mini-bigdata-003:19000
wget mini-bigdata-004:19000

