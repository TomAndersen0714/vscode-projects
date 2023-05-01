// NOTE: ES6 module style
// import {printTypeAndValue} from '../../utils/utils.js';

// NOTE: You can use CommonJS modules in ES6 module,
//  but you cannot use ES6 modules in CommonJS module.
import {printTypeAndValue} from '../../utils/utils.cjs';
import x from '../../utils/utils.js';


printTypeAndValue("Tom Andersen");
console.log("x = " + (x));