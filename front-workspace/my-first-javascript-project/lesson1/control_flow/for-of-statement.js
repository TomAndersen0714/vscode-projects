// https://www.w3schools.com/js/js_loop_forof.asp

// NOTE: The JavaScript for of statement loops through the values of an iterable object.
/*
for (variable of iterable) {
  // code block to be executed
}
 */

// for-of array object
const numbers = [45, 4, 9, 16, 25];
for (let num of numbers) {
    // Each iteration returns a key (x)
    console.log("num = " + (num));
}
console.log();

// for-of string
const text = "Hello, world!";
for (let textElement of text) {
    console.log("textElement = " + (textElement));
}
console.log();