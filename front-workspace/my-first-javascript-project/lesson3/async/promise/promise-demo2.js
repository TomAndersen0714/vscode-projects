// initial promise, using static method Promise.resolve()
const promise1 = Promise.resolve("Hello World!");


// promise chain
const promise2 = promise1.then(
    (value) => {
        console.log("promise2 value = " + (value));
        return value;
    }
);

const promise3 = promise2.then(
    // result of this promise will be undefined, if no return in onfulfilled callback function
    (value) => console.log("promise3 value = " + (value))
);

// result of this promise will be undefined, cause previous promise has no return in onfulfilled
const promise4 = promise3.then(
    (value) => {
        console.log("promise4 value = " + (value));
        return value;
    }
);