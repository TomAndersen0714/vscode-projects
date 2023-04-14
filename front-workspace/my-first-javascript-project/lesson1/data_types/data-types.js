// NOTE= JavaScript is dynamic type language, which has six data types.

// Numbers
let length = 15;
let weight = 7.1;
console.log("length = " + length);
console.log("weight = " + weight);

// Strings
let color = "yellow"; // single quotes equals to double quotes
let lastName = 'Andersen';
let title = 'Hello "Jim"!';
console.log("color = " + color);
console.log("lastName = " + lastName);
console.log("title = " + title);

// Booleans
let x = true;
let y = !x;
console.log("x = " + x);
console.log("y = " + y);


// Object
const person = {firstName: "Tom", lastName: "Andersen"};
console.log("person = " + person);

// Array Object
const cars = ["Saab", "Volvo", "BMW"];
console.log("cars = " + cars);

// Date Object
const date = new Date("2023-04-14");
console.log("date = " + date);

// NOTE= data type automatically convert
let v = 16 + "Hello";
console.log("v = " + v);
