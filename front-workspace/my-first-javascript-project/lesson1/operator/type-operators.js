// typeof
// returns the type of variable
console.log("typeof 'John' = " + typeof 'John');

console.log("typeof 'John' = " + (typeof 'John'));                 // Returns "string"
console.log("typeof 3.14 = " + (typeof 3.14));                   // Returns "number"
console.log("typeof NaN = " + (typeof NaN));                    // Returns "number"
console.log("typeof false = " + (typeof false));                  // Returns "boolean"
console.log("typeof [1, 2, 3, 4] = " + (typeof [1, 2, 3, 4]));              // Returns "object"
console.log("typeof {name: 'John', age: 34} = " + (typeof {name: 'John', age: 34}));  // Returns "object"
console.log("typeof new Date() = " + (typeof new Date()));             // Returns "object"
console.log("typeof function () {} = " + typeof function () {
});         // Returns "function"
console.log("typeof myCar = " + (typeof myCar));                  // Returns "undefined" *
console.log("typeof null = " + (typeof null));                   // Returns "object"
console.log();

// instanceof
// returns true if an object is an instance of an object type
let a = [1, 'a'];
console.log("typeof a = " + (typeof a));
console.log("a instanceof Object = " + (a instanceof Object));
console.log();

// The constructor Property
console.log("'John'.constructor = " + ('John'.constructor));

// void
console.log("void(0) = " + (void (0)));