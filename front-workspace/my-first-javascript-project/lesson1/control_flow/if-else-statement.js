// https://www.w3schools.com/js/js_if_else.asp

// if-else statement
let hour = new Date().getHours();
console.log("hour = " + (hour));

if (hour < 18) {
    greeting = "Good day";
}
else {
    greeting = "Good evening";
}
console.log("greeting = " + (greeting));


if (hour < 10) {
    greeting = "Good morning";
}
else if (hour < 20) {
    greeting = "Good day";
}
else {
    greeting = "Good evening";
}
console.log("greeting = " + (greeting));