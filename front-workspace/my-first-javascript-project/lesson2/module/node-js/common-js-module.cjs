// https://nodejs.org/docs/latest-v18.x/api/modules.html
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Modules
// https://www.w3schools.com/js/js_modules.asp

// NOTE: CommonJS module style in Node.js


const module_specifier = "../../utils/utils.cjs";
const utils = require(module_specifier);
utils.printTypeAndValue("Tom Andersen");