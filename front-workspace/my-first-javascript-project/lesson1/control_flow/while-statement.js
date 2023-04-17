// https://www.w3schools.com/js/js_loop_while.asp

// NOTE: do-while statement
let text = "", i = 0;
do {
    text += "\nThe number is " + i;
    i++;
} while (i < 10);

console.log("text = " + (text));
console.log();

// NOTE: while-do statement
const cars = ["BMW", "Volvo", "Saab", "Ford"];
text = "";
i = 0;
while (cars[i]) {
    text += cars[i] + " ";
    i++;

    // NOTE: continue statement
    if (i === 10) continue;
    // NOTE: break statement
    if (i === 3) break;
}

console.log("text = " + (text));
console.log();
