// https://www.w3schools.com/js/js_object_prototypes.asp

// All JavaScript objects inherit properties and methods from a prototype(i.e. super class).
function Person(first, last, age, eyecolor) {
    this.firstName = first;
    this.lastName = last;
    this.age = age;
    this.eyeColor = eyecolor;
}


// you can not add a new property to an existing object constructor:
const myFather = new Person("John", "Doe", 50, "blue");
Person.nationality = "English";
console.log("Person.nationality = " + (Person.nationality));
const myMother = new Person("Sally", "Rally", 48, "green");
console.log("myMother.nationality = " + (myMother.nationality));

// NOTE: Date objects inherit from Date.prototype, Array objects inherit from Array.prototype
//  Object.prototype is on the top of the prototype inheritance chain.
console.log(Date.prototype);
console.log("Array.prototype = " + (Array.prototype));
console.log("Object.prototype = " + (Object.prototype));


// NOTE: Using prototype property can add methods to object constructor,
//  which also reflects on the objects.
Person.prototype.name = function () {
    return this.firstName + " " + this.lastName;
};
console.log("myMother.name() = " + (myMother.name()));

// NOTE: Using prototype property can add properties to object constructor,
//  which also reflects on the objects.
Person.prototype.nationality = "English";
console.log("myMother.nationality = " + (myMother.nationality));