// https://www.w3schools.com/js/js_json.asp

let text = '{ "employees" : [' + '{ "firstName":"John" , "lastName":"Doe" },' + '{ "firstName":"Anna" , "lastName":"Smith" },' + '{ "firstName":"Peter" , "lastName":"Jones" } ]}';

// JSON.parse()
// Converts a JavaScript Object Notation (JSON) string into an object.
const JSONObj = JSON.parse(text);

// JSON.stringify(), json to string
console.log("JSON.stringify(JSONObj) = " + (JSON.stringify(JSONObj)));
// JSON.stringify(), format string
console.log("JSON.stringify(JSONObj, null, 4) = " + (JSON.stringify(JSONObj, null, 4)));