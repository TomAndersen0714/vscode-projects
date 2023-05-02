import axios from 'axios';

// 使用 axios 发送 HTTP 请求
axios.get('https://www.baidu.com')
    .then(response => console.log(response.data))
    .catch(error => console.error(error));
