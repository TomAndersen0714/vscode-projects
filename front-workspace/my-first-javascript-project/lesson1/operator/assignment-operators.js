/*
Operator    Example     Same As
=           x = y       x = y
+=          x += y      x = x + y
-=          x -= y      x = x - y
*=          x *= y      x = x * y
/=          x /= y      x = x / y
%=          x %= y      x = x % y
**=         x **= y     x = x ** y
 */

// The Assignment Operator (=) assigns a value to a variable
// Assign the value 5 to x
let x = 5;
// Assign the value 2 to y
let y = 2;
// Assign the value x + y to z:
let z = x + y;


// The Addition Assignment Operator (+=) adds a value to a variable.
console.log("x = " + (x));
console.log("x += 5 = " + (x += 5));

// -=
console.log("x -= 2 = " + (x -= 2));

// *=
console.log("x *= 2 = " + (x *= 2));

// /=
console.log("x /= 2 = " + (x /= 2));

// %=
console.log("x %= 3 = " + (x %= 3));

// **=
console.log("x **= 2 = " + (x **= 2));