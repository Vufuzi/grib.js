var gribParse = require('./lib/parser.js');
var tables = require('./lib/tables.js');

for(var tableName in tables.tables) {
  exports[tableName] = tables.tables[tableName];
}

/**
 *
 *
 * @param {Buffer} data
 * @param {Function} cb
 * @returns
 */
function readData (data, cb) {
  let msgs;

  // Write the contents of the buffer catching any parse errors
  try {
    msgs = gribParse.parseBuffer(data);
  } catch (e) {
    return cb(e, null);
  }

  // If no messages were parsed throw an error
  if (msgs.length == 0) { return cb(new Error('No GRIB messages could be decoded')); }

  cb(null, msgs);
}

exports.readData = readData;
