// https://www.w3schools.com/js/js_loop_forin.asp

// NOTE: The JavaScript for in statement loops through the properties of an Object.
// for-in array object
const numbers = [45, 4, 9, 16, 25];
for (let num in numbers) {
    // Each iteration returns a key (x)
    console.log("num = " + (num));
}
console.log();

// for-in string
// Each iteration returns index of character
const text = "Hello, World!";
for (let c in text) {
    console.log("c = " + (c));
    console.log("text.at(c) = " + (text.at(c)));
}
console.log();

// for-in object
// Each iteration returns a key of object
const person = {firstName: "Tom", lastName: "Andersen"};
for (let name in person) {
    console.log("name = " + (name));
    console.log("person[name] = " + (person[name]));
}