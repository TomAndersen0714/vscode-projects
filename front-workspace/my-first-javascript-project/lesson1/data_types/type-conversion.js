// https://www.w3schools.com/js/js_type_conversion.asp

const utils = require("../../utils/utils");

// NOTE: Number conversion
// Number()
console.log("Number(1.2) = " + (Number(1.2)));
utils.printTypeAndValue(Number(1.2));
// parseFloat()
console.log("parseFloat('1.23') = " + (parseFloat('1.23')));
// parseInt()
console.log("parseInt('132') = " + (parseInt('132')));
console.log();


// NOTE: String conversion
let x = 1;
console.log("String(x) = " + (String(x)));         // returns a string from a number variable x
console.log("String(123) = " + (String(123)));       // returns a string from a number literal 123
console.log("String(100 + 23) = " + (String(100 + 23)));  // returns a string from a number from an expression

