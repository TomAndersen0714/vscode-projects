fetch("https://galaxy.xiaoduoai.com/api/query_results", {
  "headers": {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "content-type": "application/json;charset=UTF-8",
    "sec-ch-ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin"
  },
  "referrer": "https://galaxy.xiaoduoai.com/queries/new",
  "referrerPolicy": "strict-origin-when-cross-origin",
  "body": "{\"data_source_id\":13,\"parameters\":{},\"query\":\"SELECT *\\nFROM ft_dwd.order_detail_all\\nWHERE day = 20230212\\nLIMIT 11\",\"max_age\":0}",
  "method": "POST",
  "mode": "cors",
  "credentials": "include"
}); ;


fetch("https://galaxy.xiaoduoai.com/api/jobs/78a33bd3-8e95-4b91-94ed-233c377cba1e", {
  "headers": {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "sec-ch-ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin"
  },
  "referrer": "https://galaxy.xiaoduoai.com/queries/new",
  "referrerPolicy": "strict-origin-when-cross-origin",
  "body": null,
  "method": "GET",
  "mode": "cors",
  "credentials": "include"
}); ;


fetch("https://galaxy.xiaoduoai.com/api/query_results/1973179", {
  "headers": {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "sec-ch-ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin"
  },
  "referrer": "https://galaxy.xiaoduoai.com/queries/new",
  "referrerPolicy": "strict-origin-when-cross-origin",
  "body": null,
  "method": "GET",
  "mode": "cors",
  "credentials": "include"
}); ;


fetch("https://galaxy.xiaoduoai.com/api/events", {
  "headers": {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "content-type": "application/json;charset=UTF-8",
    "sec-ch-ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin"
  },
  "referrer": "https://galaxy.xiaoduoai.com/queries/new",
  "referrerPolicy": "strict-origin-when-cross-origin",
  "body": "[{\"action\":\"execute\",\"object_type\":\"query\",\"timestamp\":1676367452.326,\"screen_resolution\":\"1463x915\"}]",
  "method": "POST",
  "mode": "cors",
  "credentials": "include"
});