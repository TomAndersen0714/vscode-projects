// https://www.w3schools.com/js/js_arrays.asp
// https://www.w3schools.com/js/js_array_methods.asp

// Array Object
const cars = ["Saab", "Volvo", "BMW"];
console.log("typeof cars = " + typeof cars);
console.log("cars = " + cars);
console.log();

// access Array
console.log("cars.at(0) = " + cars.at(0));
console.log("cars[0] = " + cars[0]);
console.log();

// NOTE: Array Method
// forEach()
function handler(p) {
    console.log("p = " + (p));
}

cars.forEach(handler);