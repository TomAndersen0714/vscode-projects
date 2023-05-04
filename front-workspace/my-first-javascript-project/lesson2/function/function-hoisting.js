// https://www.w3schools.com/js/js_function_definition.asp

// NOTE: Function Hoisting
// JavaScript function和var声明的变量一样, 会发生提升(Hoisting),
// 即function的声明不论在哪里都会被提取到对应作用域的顶部, 但还是建议声明在调用之前.
console.log("addFunc(1,2) = " + (addFunc(1, 2)));

function addFunc(p1, p2) {
    return p1 + p2;
}





