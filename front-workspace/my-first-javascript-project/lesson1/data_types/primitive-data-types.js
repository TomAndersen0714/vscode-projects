/*
NOTE: JavaScript is dynamic type language, which has 8 data types.
1. string
2. number
3. bigint
4. boolean
5. undefined
6. null
7. object
8. symbol
*/


// String
let color = "yellow"; // single quotes equals to double quotes
let lastName = 'Andersen';
let title = 'Hello "Jim"!';
console.log("typeof color = " + typeof color);
console.log("color = " + color);
console.log("typeof lastName = " + typeof lastName);
console.log("lastName = " + lastName);
console.log("typeof title = " + typeof title);
console.log("title = " + title);
console.log();

// Number
// NOTE: All number is IEEE754 64-bit float
let length = 15;
let weight = 7.1;
console.log("typeof length = " + typeof length);
console.log("length = " + length);
console.log("typeof weight = " + typeof weight);
console.log("weight = " + weight);
console.log();

// Bigint
let num = BigInt("123456789012345678901234567890");
console.log("typeof num = " + typeof num);
console.log("num = " + num);
console.log();

// Boolean
let x = true;
let y = !x;
console.log("typeof x = " + typeof x);
console.log("x = " + x);
console.log("typeof y = " + typeof y);
console.log("y = " + y);
console.log();

// Object
const person = {firstName: "Tom", lastName: "Andersen"};
console.log("typeof person = " + typeof person);
console.log("person = " + person);
console.log("person.firstName = " + person.firstName);
console.log("person.lastName = " + person.lastName);
console.log();

let obj = {count: 1};
console.log("obj.count = " + (obj.count));

// Undefined
let a;
console.log("typeof a = " + typeof a);
console.log("a = " + a);
a = undefined;
console.log("typeof a = " + typeof a);
console.log("a = " + a);
console.log();

// Null
let b = null;
console.log("typeof b = " + typeof b);
console.log("b = " + b);
console.log();

// Symbol
let s = Symbol();
console.log("typeof s = " + typeof s);
console.log();

// NOTE: data type automatically convert
let v = 16 + "Hello";
console.log("typeof v = " + typeof v);
console.log("v = " + v);


