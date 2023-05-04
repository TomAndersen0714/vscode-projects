// https://www.w3schools.com/js/js_arrow_function.asp

// NOTE: Arrow Function / Anonymous Function
// JavaScript Arrow Function is just like lambda expression, which is used to create
// anonymous function.
let myFunc = (a, b) => a * b;
console.log("myFunc(3, 4) = " + (myFunc(3, 4)));


// using rest parameter
const addNums = ((...args) => {
    let sum = 0;
    for (let arg of args) {
        sum += arg;
    }
    return sum;
});

console.log("addNums(2, 3, 4, 5) = " + addNums(2, 3, 4, 5));