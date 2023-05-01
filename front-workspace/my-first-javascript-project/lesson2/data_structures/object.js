// https://www.w3schools.com/js/js_objects.asp

// NOTE: Object

import {printTypeAndValue} from "../../utils/utils.js";

const person = {firstName: "Tom", lastName: "Andersen"};
console.log("typeof person = " + typeof person);
console.log("person = " + person);
console.log();

// access member of Object
console.log("person.firstName = " + person.firstName);
console.log("person.lastName = " + person.lastName);

// for-in statement
for (let p in person) {
    console.log("p = " + (p));
}


// NOTE: Object property , PS: object method is also a kind of property 
const app = {
    // object property 
    firstName: "John", lastName: "Wick",

    // object method
    data() {
    }, name() {
        // NOTE: In an object method, 'this' refers to the current object.
        return this.firstName + " " + this.lastName;
    },

    // object property 
    property: {
        // object method
        name() {
            return this.firstName + " " + this.lastName;
        }
    }
};


console.log("app = " + (app));
console.log("app.data = " + (app.data));
console.log("app.attributes = " + (app.property));
console.log("app.attributes.method = " + (app.property.method));

console.log("app.name() = " + (app.name()));
console.log("app.property .name() = " + (app.property.name()));

printTypeAndValue((app));
printTypeAndValue((app.data));
printTypeAndValue((app.property));
printTypeAndValue((app.property.method));