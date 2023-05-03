// https://www.ruanyifeng.com/blog/2020/08/how-nodejs-use-es6-module.html
function printTypeAndValue(value) {
    console.log("type: " + typeof value + ", value: " + value);
}

// NOTE: Named export
export {printTypeAndValue};

// NOTE: Default export
const a = 1;
export default a;

// NOTE: Module source code will be executed when module loaded.
// console.log("a = " + (a));

// NOTE: A module cannot have multiple default exports
// let b = 1;
// export default b;