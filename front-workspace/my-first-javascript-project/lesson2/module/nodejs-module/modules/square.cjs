// Assigning to exports will not modify module, must use module.exports
module.exports.Square = class Square {
    constructor(width) {
        this.width = width;
    }

    area() {
        return this.width ** 2;
    }
};

// console.log("module.exports.Square = " + (module.exports.Square));
// console.log("new module.exports.Square(2) = " + (new module.exports.Square(2)));