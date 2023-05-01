// https://www.ruanyifeng.com/blog/2020/08/how-nodejs-use-es6-module.html
function printTypeAndValue(value) {
    console.log("type: " + typeof value + ", value: " + value);
}

export {printTypeAndValue};


let a = 1;
export default a;

// A module cannot have multiple default exports
// let b = 1;
// export default b;