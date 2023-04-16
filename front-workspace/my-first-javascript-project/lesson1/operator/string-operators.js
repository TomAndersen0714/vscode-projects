// The add operator(+) can also be used to concatenate strings
// NOTE: For each variable that is not a String, its toString() method is called to convert it to a String.
let firstName = "Tom";
let lastName = "Andersen";
console.log("firstName + ' ' + lastName = " + (firstName + ' ' + lastName));

// The += assignment operator can also be used to add (concatenate) strings
let text1 = "What a very ";
console.log("text1 = " + (text1));
console.log("text1 += 'nice day' = " + (text1 += 'nice day'));
