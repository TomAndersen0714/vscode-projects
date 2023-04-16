// https://www.runoob.com/nodejs/nodejs-module-system.html
function printTypeAndValue(value) {
    console.log("type: " + typeof value + ", value: " + value);
}

module.exports = {
    printTypeAndValue: printTypeAndValue
};