// https://www.w3schools.com/js/js_object_constructors.asp


// Object constructor function
function Person(first, last, age, eye) {
    this.firstName = first;
    this.lastName = last;
    this.age = age;
    this.eyeColor = eye;
}


const myFather = new Person("John", "Doe", 50, "blue");
const myMother = new Person("Sally", "Rally", 48, "green");

console.log("myFather = " + (myFather));
console.log("myMother = " + (myMother));


// Adding a Method to an Object
myFather.name = function () {
    return this.firstName + " " + this.lastName;
};


// NOTE: You cannot add a new property to an object constructor the same way
//  you add a new property to an existing object
// Adding a Property to a Constructor, PS: this does not work!
Person.nationality = "English";
console.log("myFather.nationality = " + (myFather.nationality));

// Adding a Method to a Constructor
function Person1(first, last, age, eyecolor) {
    this.firstName = first;
    this.lastName = last;
    this.age = age;
    this.eyeColor = eyecolor;
    this.name = function () {
        return this.firstName + " " + this.lastName;
    };
}