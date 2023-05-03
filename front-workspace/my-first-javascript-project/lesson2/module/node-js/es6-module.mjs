// https://nodejs.org/dist/latest-v18.x/docs/api/esm.html
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Modules
// https://www.w3schools.com/js/js_modules.asp

// NOTE: ES6 module style in Node.js

// 1. Relative specifiers, which must contain file extension, i.e. '.js'
import {printTypeAndValue} from '../../../utils/utils.js';

// 2. Absolute specifiers, which must contain file extension, i.e. '.js'
import AxiosError from '/WorkSpace/projects/VSCodeProjects/front-workspace/node_modules/axios/lib/axios.js';
// import axios from '/WorkSpace/projects/VSCodeProjects/front-workspace/node_modules/axios/lib/axios.js';

// 3. Bare specifiers
// NOTE: Like in CommonJS, module files within packages can be accessed by appending a path to the package name (i.e. bare specifiers)
//  unless the package's package.json contains an "exports" field, in which case files within packages can
//  only be accessed via the paths defined in "exports" field of package's package.json.
import Axios from "axios";
import * as settle from "axios/unsafe/core/settle.js";


// NOTE: Import assertions (Experimental)
import pkg from "axios/package.json" assert {type: 'json'};
// format json object
console.log("pkg = " + (JSON.stringify(pkg, null, 4)));

// NOTE: You can use CommonJS modules in ES6 module statement,
//  but you cannot use ES6 modules in CommonJS module statement.
import x from '../../../utils/utils.cjs';
import formDataToJSON from "axios/unsafe/helpers/formDataToJSON.js";

printTypeAndValue("Tom Andersen");
console.log("x = " + (x));

// NOTE: import Specifiers of ES6 module
//  NODE_PATH is not part of resolving `import specifiers`, but useful for
//  resolving `require specifiers`.

// NOTE: 在 https://v8.dev/features/modules#mjs v8文档中, 建议将 JavaScript ES Module 源文件使用
//  .mjs后缀来命名, 来提高可读性, 但是在 https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Modules#aside_%E2%80%94_.mjs_versus_.js
//  MDN文档中, 为了保持可移植性, 还是建议保留 .js 后缀, 在Chrome浏览器中, 两种格式都可, 但为了保持可移植性, 还是
//  建议保留 .js 文件后缀风格, 此处这种格式仅作展示用.