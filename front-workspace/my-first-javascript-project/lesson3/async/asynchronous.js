// https://www.w3schools.com/js/js_asynchronous.asp

// setTimeout()
setTimeout(() => {
    hello("I love You !!!");
}, 3000);

function hello(value) {
    console.log(value);
}

// setInterval()
setInterval(myFunction, 1000);

function myFunction() {
    let d = new Date();
    console.log(d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds());
}