// https://www.w3schools.com/js/js_function_definition.asp

// Self-Invoking Functions,
// also called IIFE (Immediately Invoked Function Expression), https://developer.mozilla.org/en-US/docs/Glossary/IIFE
// NOTE: A self-invoking expression is invoked (started) automatically, without being called.
//  Just like a anonymous one-time called function.
(function () {
    let x = "Hello!!";  // I will invoke myself
    console.log("x = " + (x));
})();
