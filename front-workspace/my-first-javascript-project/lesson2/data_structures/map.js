// https://www.w3schools.com/js/js_maps.asp


// NOTE: Map methods

// Map()
const fruits = new Map([["apples", 500], ["bananas", 300], ["oranges", 200]]);

const fruits1 = new Map(fruits);

// set()
console.log("fruits.set('apples', 400) = " + (fruits.set('apples', 400)));

// get()
console.log("fruits.get('apples') = " + (fruits.get('apples')));
console.log("fruits.get('apple') = " + (fruits.get('apple')));

// delete()
console.log("fruits.delete('apple') = " + (fruits.delete('apple')));
console.log("fruits.delete('apples') = " + (fruits.delete('apples')));

// has()
console.log("fruits.has('bananas') = " + (fruits.has('bananas')));

// entries()
let entries = fruits.entries();
for (let entry of entries) {
    console.log("entry = " + (entry));
}
console.log();

// NOTE: Map Properties
console.log("fruits.size = " + (fruits.size));