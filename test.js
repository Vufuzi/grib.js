const { readData } = require('./index.js');
const fs = require('fs');

(() => {
  const buffer = fs.readFileSync('./test.grib');

  readData(buffer, (err, what) => {
    fs.writeFileSync('./test.json', JSON.stringify(what, null, 4));
    // fs.writeFileSync('./test.bmp', what['6'].data);
  });
})();