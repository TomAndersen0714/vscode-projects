// NOTE, A JavaScript Promise object can be in one of these states:
//  1. Pending, 2. Fulfilled, 3. Rejected.

// "Initial Promise" (process data, then set the initial result and state)
let myPromise = new Promise(
    /*
    NOTE:
     myResolve and myReject method is just alias name, i.e. parameter.
     And then the executor method will be invoked by passing Promise.resolve() and Promise.reject()
     methods as arguments which is already predefined in prototype.
    */
    (myResolve, myReject) => {
        let x = 0;

        if (0 === x) {
            setTimeout(
                function () {
                    // if successful, state=Fulfilled
                    myResolve("Hello World!");
                },
                1000
            );
        }
        else {
            // if failed, state=Rejected
            myReject("Error");
        }
    }
);

function myDisplayer(some) {
    console.log(some);
}

// "Promise chain", process the result of previous promise object according to the state
let myPromise1 = myPromise.then(
    (value) => {
        /* callback when successful */
        myDisplayer(value);
        return value + "!";
    },
    (error) => {
        /* callback when failed */
        myDisplayer(error);
        return error;
    }
);

// create a promise base on previous promise chain
let myPromise2 = myPromise1.then(
    (value) => {
        myDisplayer(value);
        return value + "!";
    }
);

function errorHandler(error) {
    console.error(error);
}

// create a promise base on previous promise chain
let myPromise3 = myPromise1.catch(
    (error) => {
        errorHandler(error);
    }
);