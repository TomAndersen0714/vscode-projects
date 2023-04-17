// https://www.w3schools.com/js/js_dates.asp

// Date Object, NOTE: default timezone of date object is local timezone.
const date = new Date("2023-04-14 01:00:00");
console.log("typeof date = " + typeof date);
console.log("date = " + date);
console.log();

// NOTE: Date Methods
// getTime(): Gets the time value in milliseconds from epoch, i.e. timestamp in milliseconds
console.log("date.getTime() = " + (date.getTime()));

// getSeconds(): Gets the seconds of a Date object, using local time
console.log("date.getSeconds() = " + (date.getSeconds()));
// getUTCSeconds(): Gets the seconds of a Date object using Universal Coordinated Time (UTC).
console.log("date.getUTCSeconds() = " + (date.getUTCSeconds()));


