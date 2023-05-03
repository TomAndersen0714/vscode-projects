// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Using_promises
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise
// https://www.w3schools.com/js/js_promise.asp


// NOTE, A JavaScript Promise object can be in one of these states:
//  1. Pending, 2. Fulfilled, 3. Rejected.

// "Initial Promise" (process data, then set the initial result and state)
let myPromise = new Promise(
    // NOTE: myResolve and myReject method is just alias name, i.e. parameter.
    //  And then the executor method will be invoked by passing Promise.resolve() and Promise.reject() methods as
    //  arguments which is already predefined in prototype.
    (myResolve, myReject) => {
        let x = 0;

        if (0 === x) {
            setTimeout(
                function () {
                    // if successful, state=Fulfilled
                    myResolve("I love You !!");
                },
                1000
            );
        }
        else {
            // if failed, state=Rejected
            myReject("Error");
        }
    });

function myDisplayer(some) {
    console.log(some);
}

// "Promise chain", process the result of previous promise object according to the state
let myPromise1 = myPromise.then(
    (value) => {
        /* callback when successful */
        myDisplayer(value);
        return value + "-1";
    },
    (error) => {
        /* callback when failed */
        myDisplayer(error);
        return error;
    }
);

let myPromise2 = myPromise1.then(
    (value) => {
        myDisplayer(value);
        return value + "-1";
    }
);