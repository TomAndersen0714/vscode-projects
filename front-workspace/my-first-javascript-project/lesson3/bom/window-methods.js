// https://www.w3schools.com/jsref/obj_window.asp


// NOTE: The window object represents an open window in browser.
//  If a document contain frames(<iframe> tags), the browser creates one window object
//  for the HTML document, and one additional window object for each frame.
function greeting() {
    console.log('Hello World!');
}

setTimeout(greeting, 500);
setTimeout(greeting, 1500);


setInterval(greeting, 1000);

