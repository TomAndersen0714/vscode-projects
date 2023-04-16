// https://www.w3schools.com/js/js_variables.asp
// Variables are containers for storing data (storing data values).

// 4 Ways to Declare a JavaScript Variable:


// 1. var
// The `var` keyword is used in all JavaScript code from 1995 to 2015.
// The `let` and `const` keywords were added to JavaScript in 2015.
// If you want your code to run in older browsers, you must use `var`.

// NOTE: "var"声明的变量是函数作用域, 或者全局作用域, 即即便是在代码块中声明, 依旧会在整个
//  函数作用域, 或者全局作用域生效.
// NOTE: var变量"提升(Hoisting)": 在执行代码之前, var修饰的变量和函数声明会移至其作用域的顶部, 并
//  使用undefined来初始化变量的值, 意味着在var修饰的变量, 在声明之前就可以使用它.
console.log("x1 = " + (x1));

var x1 = 5;
var y1 = 6;
var z1 = x1 + y1;
console.log("z1 = " + (z1));

// If you re-declare a JavaScript variable declared with var, it will not lose its value.
var x1;
console.log("x1 = " + (x1));
console.log();

// 2. let
// NOTE: since ES6
let x2 = 5;
let y2 = 6;
let z2 = x2 + y2;
console.log("z2 = " + (z2));
console.log();

// 3. const
// NOTE: since ES6
// `const` declare that the variable can not be changed
const price1 = 5;
const price2 = 6;
let total = price1 + price2;
console.log("total = " + (total));
console.log();

// 4. using nothing
x3 = 5;
y3 = 6;
z3 = x3 + y3;
console.log("z3 = " + (z3));
console.log();
