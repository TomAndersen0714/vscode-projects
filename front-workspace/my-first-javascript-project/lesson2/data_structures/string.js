// https://www.w3schools.com/js/js_string_methods.asp

// NOTE: String Methods
let text = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

// length
console.log("text.length = " + (text.length));

// slice()
// The negative number also represent an index of element
console.log("text.slice(1,2) = " + (text.slice(1, 2)));
console.log("text.slice(-2, -1) = " + (text.slice(-2, -1)));
console.log("text.slice(-8) = " + (text.slice(-8)));
console.log("text.slice(-1, 2) = " + (text.slice(-1, 2)));
console.log("text.slice(1, -1) = " + (text.slice(1, -1)));

// substring()
// The difference is that start and end values less than 0 are treated as 0 in substring().
console.log("text.substring(-1, 2) = " + (text.substring(-1, 2)));
console.log("text.substring(1, 2) = " + (text.substring(1, 2)));

// substr(), PS: Deprecated
console.log("text.substr(1, 2) = " + (text.substr(1, 2)));
console.log();

// replace()
// replaces only the first match
text = "Please visit Microsoft!";
let newText = text.replace("Microsoft", "W3Schools");
console.log("newText = " + (newText));
// replaceAll()
text = "cats dogs mouses cats dogs";
text.replaceAll("cats", "dogs");
console.log("text = " + (text));

// toUpperCase()
text1 = "Hello World!";
console.log("text1.toUpperCase() = " + (text1.toUpperCase()));
// toLowerCase()
console.log("text1.toLowerCase() = " + (text1.toLowerCase()));

// concat()
console.log("'Hello'.concat(' World!') = " + ('Hello'.concat(' World!')));

// trim()
console.log("' Hello World! '.trim() = " + (' Hello World! '.trim()));
// trimStart()
console.log("' Hello World! '.trimStart() = " + (' Hello World! '.trimStart()));
// trimEnd()
console.log("' Hello World! '.trimEnd() = " + (' Hello World! '.trimEnd()));

// padStart()
text = "5";
console.log("text.padStart(4, '3') = " + (text.padStart(4, '3')));
// padEnd()
console.log("text.padEnd(8, '2') = " + (text.padEnd(8, '2')));


// charAt(): return empty string if no character found
text = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
console.log("text.charAt(0) = " + (text.charAt(0)));
// chatCode(): returns a UTF-16 code (an integer between 0 and 65535)
console.log("text.charCodeAt(0) = " + (text.charCodeAt(0)));


// split(): convert string to array
text = "Hello World!";
strings = text.split(' ');
console.log("text.split(' ') = " + (strings));
strings.forEach((x) => console.log(x));