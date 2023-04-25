import {format} from 'sql-formatter';

console.log(format('SELECT * FROM tbl WHERE id = id', {language: 'mysql'}));


// console.log('SELECT * FROM tbl WHERE id = {{id}}');