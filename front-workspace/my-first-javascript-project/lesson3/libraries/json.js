// https://www.w3schools.com/js/js_json.asp

let text = '{ "employees" : [' + '{ "firstName":"John" , "lastName":"Doe" },' + '{ "firstName":"Anna" , "lastName":"Smith" },' + '{ "firstName":"Peter" , "lastName":"Jones" } ]}';

// JSON.parse()
const JSONObj = JSON.parse(text);

// JSON.stringify()
console.log("JSON.stringify(JSONObj) = " + (JSON.stringify(JSONObj)));