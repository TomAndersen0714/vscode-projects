// https://www.w3schools.com/js/js_sets.asp


// NOTE: Set Methods

// Set(): create a Set
const letters = new Set(["a", "b", "c"]);

import {printTypeAndValue} from "../../utils/utils.js";

printTypeAndValue(letters);
console.log();

// add(): add value to the set
letters.add("d");
console.log();

// delete(): removes an element from a Set
letters.delete("c");
console.log();

// forEach()
letters.forEach((x) => { // Arrow Functions / Lambda expression
    console.log(x);
});
console.log();

// values(): returns a new iterator object containing all the values in a Set
console.log("letters.values() = " + (letters.values()));
let text = "";
for (const x of letters.values()) {
    text += x + " ";
}
console.log("text = " + (text));

// has(): returns true if a value exists in the Set
console.log("letters.has('Tom') = " + (letters.has('Tom')));

// NOTE: Set Properties
// size
console.log("letters.size = " + (letters.size));