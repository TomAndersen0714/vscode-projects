// https://www.w3schools.com/js/js_switch.asp


let dayOfWeek = new Date().getDay();

switch (dayOfWeek) {
    case 0:
        day = "Sunday";
        break;
    case 1:
        day = "Monday";
        break;
    case 2:
        day = "Tuesday";
        break;
    case 3:
        day = "Wednesday";
        break;
    case 4:
        day = "Thursday";
        break;
    case 5:
        day = "Friday";
        break;
    case 6:
        day = "Saturday";
        break;
    default:
        day = null;
        break;
}
console.log("day = " + (day));


// Sometimes you will want different switch cases to use the same code.
// In this example case 4 and 5 share the same code block, and 0 and 6 share another code block:
switch (new Date().getDay()) {
    case 4:
    case 5:
        text = "Soon it is Weekend";
        break;
    case 0:
    case 6:
        text = "It is Weekend";
        break;
    default:
        text = "Looking forward to the Weekend";
}