const { readData } = require('./index.js');
const fs = require('fs');

(() => {
  const buffer = fs.readFileSync('./test.grib');

  readData(buffer, (err, what) => {
    console.log({ err, what });
  });
})();