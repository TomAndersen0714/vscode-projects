/*
* https://www.w3schools.com/js/js_async.asp
* https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/await
*
* */

// NOTE: await is a kind of operator which can only be use inside an `async function` or
//  at the top level of a JavaScript module.

function foo() {
    console.log("starting slow promise");
    return new Promise((resolve) => {
        // set a timer
        setTimeout(() => {
            resolve("slow");
            console.log("slow promise is done");
        }, 2000);
    });
}

// NOTE: await expression will wait and return the fulfillment value of the promise or thenable object,
const a = await foo();
// NOTE: or, just return the expression's own value if the expression is not thenable.
const b = await 1;

console.log("a = " + (a));
console.log("b = " + (b));


function resolveAfter2Seconds(x) {
    return new Promise((resolve) => {
        setTimeout(() => {
            resolve(x);
        }, 2000);
    });
}

async function f1() {
    const x = await resolveAfter2Seconds(10);
    console.log(x); // 10
}

f1();
