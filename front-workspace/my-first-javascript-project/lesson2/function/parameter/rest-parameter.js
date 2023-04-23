// https://www.w3schools.com/js/js_function_parameters.asp


// NOTE: Rest Parameter
function sum(...args) {
    let sum = 0;
    for (let arg of args) sum += arg;
    return sum;
}

console.log("sum(4, 9, 16, 25, 29, 100, 66, 77) = " +
    (sum(4, 9, 16, 25, 29, 100, 66, 77)));