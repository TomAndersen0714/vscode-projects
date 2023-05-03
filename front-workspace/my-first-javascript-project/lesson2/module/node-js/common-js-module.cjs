// https://nodejs.org/docs/latest-v18.x/api/modules.html
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Modules
// https://www.w3schools.com/js/js_modules.asp

// NOTE: CommonJS module style in Node.js

// NOTE: CommonJS module require specifiers
// 1. Relative specifiers, which must contain file extension, i.e. '.cjs'
// A required module prefixed with './' is relative to the file calling require().
// That is, circle.js must be in the same directory as foo.js for require('./circle') to find it.
const circle = require('./modules/circle.cjs');
console.log(`The area of a circle of radius 4 is ${circle.area(4)}`);

// 2. Absolute specifiers
// A required module prefixed with '/' is an absolute path to the file.
// For example, require('/home/marco/foo.js') will load the file at /home/marco/foo.js.
const axios1 = require("/WorkSpace/projects/VSCodeProjects/front-workspace/my-first-javascript-project/lesson2/module/node-js/modules/circle.cjs");

// 3. Bare specifiers
// Without a leading '/', './', or '../' to indicate a file, the module must either be a core module or is loaded from a node_modules folder
// https://nodejs.org/docs/latest-v18.x/api/modules.html#loading-from-node_modules-folders
const axios2 = require("axios");

// PS: All specifiers, https://nodejs.org/docs/latest-v18.x/api/modules.html#all-together


// NOTE: NODE_PATH environment, only valid in require specifiers of commonJS module
// If the NODE_PATH environment variable is set to a colon-delimited list of absolute paths,
// then Node.js will search those paths for modules if they are not found elsewhere.
// https://nodejs.org/docs/latest-v18.x/api/modules.html#loading-from-the-global-folders


// NOTE: import() expression, dynamic import module
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/import
console.log();
let module_specifier = "../../../utils/utils.cjs";
import(module_specifier)
    .then((module) => {
        // 使用 module 中导出的内容
        console.log("JSON.stringify(module) = " + (JSON.stringify(module)));
        console.log("module.default = " + (module.default));
        console.log("module.printTypeAndValue = " + (module.printTypeAndValue));
        module.printTypeAndValue(module.default);
    })
    .catch((error) => {
        // 处理错误
        console.log("error = " + (error));
    });
