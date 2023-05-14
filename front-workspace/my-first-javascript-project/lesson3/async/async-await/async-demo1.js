/*
* https://www.w3schools.com/js/js_async.asp
* https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/async_function
* */

// NOTE: `Async function` is a kind of function which is decorated with `async operator`, and
//  will return a Promise object wrapping the returned value.

let p = Promise.resolve(1);

async function asyncFunc() {
    return p;
}

// above declaration is similar to below, but do not equal
function basicReturn() {
    return Promise.resolve(p);
}

console.log("p===asyncFunc() = " + (p === asyncFunc()));
console.log("p===basicReturn() = " + (p === basicReturn()));


