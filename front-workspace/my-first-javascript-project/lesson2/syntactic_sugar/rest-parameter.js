// NOTE: rest parameter
const addNums = ((...args) => {
    let sum = 0;
    for (let arg of args) {
        sum += arg;
    }
    return sum;
});

console.log("addNums(2, 3, 4, 5) = " + addNums(2, 3, 4, 5));