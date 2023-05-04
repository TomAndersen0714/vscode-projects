// https://www.w3schools.com/js/js_function_definition.asp

// NOTE: Function Constructor
const myFunction = new Function("a", "b", "return a*b");

console.log("myFunction(4, 3) = " + (myFunction(4, 3)));