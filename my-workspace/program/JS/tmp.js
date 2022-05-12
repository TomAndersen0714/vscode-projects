var url = "https://coolapi.coolcollege.cn/enterprise-api/v2/1456280502347632708/users/1829102005948387328/studies/1837029349853892608/courses/1837030365768847360/resources/1837023407863107584/save_progress";
var params = {
  access_token: "137d9bb17be64728b726f7bbd2a8acb7",
  progress: 99,
  recent_start: 1259,
  tempTime: 1652268296000
};

var xhr = new XMLHttpRequest();
xhr.open("POST", url, true);
xhr.setRequestHeader("Content-Type", "application/json");
// xhr.onload = function (e) {
//   if (xhr.readyState === 4) {
//     if (xhr.status === 200) {
//       console.log(xhr.responseText);
//     } else {
//       console.error(xhr.statusText);
//     }
//   }
// };
// xhr.onerror = function (e) {
//   console.error(xhr.statusText);
// };
xhr.send(JSON.stringify(params));