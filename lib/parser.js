var tables = require('./tables');

/**
 *
 *
 * @param {Buffer} buffer
 * @returns
 */
function parseBuffer(buffer) {
  var msgs = [],
    offset,
    indicator,
    sections;

  // GRIB messages start with 'GRIB'
  for (offset = 0; offset <= buffer.byteLength - 4; offset++) {
    if (buffer.readUInt32BE(offset) == 0x47524942) {
      const indicatorBufferPart = buffer.subarray(offset, 16);
      const { byteLength } = parseIndicator(indicatorBufferPart);

      const restOfBuffer = buffer.subarray(offset + 16);
      // If we found a message, parse it into sections and add to messages
      sections = parseSections(restOfBuffer);
      msgs.push(new Grib2Message(sections));

      // Move to last byte of message (the offset++ in the for loop will move us to next one)
      offset += byteLength - 1;
    }
  }

  return msgs;
}

exports.parseBuffer = parseBuffer;

var sectionKeys = [
  'indicator', 'identification', 'localUse',
  'grid', 'product', 'representation', 'bitMap',
  'data',
];

// An object representing a GRIB message
var Grib2Message = function(sections) {
  var currentField = { }, sectionIdx, section, key;

  this.fields = [];

  for(sectionIdx in sections) {
    section = sections[sectionIdx];

    // Is this the identification?
    if(section.number == 1) {
      // Move fields from identification to this object.
      for(key in section.contents) { this[key] = section.contents[key]; }
    } else {
      // Otherwise, add to the current field
      currentField[sectionKeys[section.number]] = section.contents;
    }

    if(section.number == 7) {
      // This is the data section, the last one in a repeated set
      this.fields.push(new Field(currentField));
    }
  }
}

var Field = function(sections) {
  var key;

  // Move fields from sections to this object
  for(key in sections) { this[key] = sections[key]; }
}

// Parse a GRIB 2 Indicator (0) section

/** @typedef Section0
 * @prop {number[]} reserved Reserved
 * @prop {number} discipline GRIB Master Table Number (see Code Table 0.0)
 * @prop {number} edition GRIB Edition Number (currently 2)
 * @prop {number} byteLength Total length of GRIB message in octets (including Section 0)
 */

/**
 * @param {Buffer} binary
 * @returns {Section0}
 */
function parseIndicator (binary) {
  const magic = binary.readUInt32BE(0);

  if (magic != 0x47524942) {
    throw new Error('Invalid magic number for indicator: ' + magic);
  }

  const reserved = [binary.readUInt8(4), binary.readUInt8(5)];
  const discipline = binary.readUInt8(6);
  const edition = binary.readUInt8(7);
  const byteLength = binary.readBigUInt64BE(8);

  if (edition !== 2) {
    throw new Error(`Unknown GRIB edition: ${edition}. Only version 2 is supported.`);
  }

  return {
    reserved,
    discipline,
    edition,
    byteLength: Number(byteLength)
  };
};

// Parse sections until end marker
var parseSections = function(binary) {
  const section1 = parseSection(binary, 0);
  const section2 = parseSection(binary, section1.byteLength);
  const section3 = parseSection(binary, section1.byteLength + section2.byteLength);
  const section4 = parseSection(binary, section1.byteLength + section2.byteLength + section3.byteLength);
  const section5 = parseSection(
    binary,
    section1.byteLength +
    section2.byteLength +
    section3.byteLength +
    section4.byteLength
  );

  return [section1, section2, section3, section4, section5];
}

// Parse a single section and advance binary to the section's end
var parseSection = function(binary, startOffset = 0) {
  binary = binary.subarray(startOffset);

  var section = {};
  let sectionParseFunc;

  section.byteLength = binary.readUInt32BE(0);
  section.number = binary.readUInt8(4);

  // Do we have a parse function for this section?
  sectionParseFunc = sectionParsers[section.number];

  if (sectionParseFunc) {
    section.contents = sectionParseFunc(binary, section.byteLength);
  }

  return section;
}

var sectionParsers = {
  1: function(binary) {
    var rv = {};
    rv.originatingCenter = binary.readUInt16BE(5);
    rv.originatingSubCenter = binary.readUInt16BE(7);
    rv.masterTablesVersion = binary.readUInt8(9);
    rv.localTablesVersion = binary.readUInt8(10);
    rv.referenceTimeSignificance = tables.lookup(
      tables.tables.ReferenceTimeSignificance,
      binary.readUInt8(11)
    );
    rv.referenceTime = new Date(
      binary.readUInt16BE(12), // year
      binary.readUInt8(14) - 1, // month
      binary.readUInt8(15), // day
      binary.readUInt8(16), // hour
      binary.readUInt8(17), // minute
      binary.readUInt8(18) // second
    );
    rv.productionStatus = tables.lookup(
      tables.tables.ProductionStatus,
      binary.readUInt8(19)
    );
    rv.type = tables.lookup(tables.tables.Type, binary.readUInt8(20));
    return rv;
  },
  3: function(binary) {
    var rv = {};
    rv.source = binary.readUInt8(5);
    rv.dataPointCount = binary.readUInt32BE(6);
    rv.pointCountOctets = binary.readUInt8(10);
    rv.pointCountInterpretation = binary.readUInt8(11);
    rv.templateNumber = binary.readUInt16BE(12);

    // Parse grid definition
    var gridDefnParser = gridParsers[rv.templateNumber];
    if (!gridDefnParser) {
      console.warn('Unknown grid definiton template: ' + rv.templateNumber);
      return rv;
    }
    rv.definition = gridDefnParser(binary);

    // FIXME: point counts

    return rv;
  },
  4: function(binary) {
    var rv = {};

    rv.numberOfCoordinatesValuesAfterTemplate = binary.readUInt16BE(5);
    rv.productDefinitionTemplateNumber = binary.readUInt16BE(7);

    // Parse grid definition
    var gridDefnParser = gridParsers[rv.productDefinitionTemplateNumber];
    if (!gridDefnParser) {
      console.warn('Unknown grid definiton template: ' + rv.templateNumber);
      return rv;
    }
    rv.definition = gridDefnParser(binary);

    return rv;
  },
  5: function(binary, byteLength) {
    // representation
    var rv = {};
    rv.dataPointCount = binary.read('uint32');
    rv.templateNumber = tables.lookup(
      tables.tables.DataRepresentationTemplateNumber,
      binary.read('uint16')
    );

    if (dataRepresentationTemplateParsers[rv.templateNumber.value]) {
      rv.details = dataRepresentationTemplateParsers[rv.templateNumber.value](
        binary
      );
    } else {
      // unknown.
      rv.data = binary.read(['blob', byteLength - 5 - 4 - 2]);
    }
    return rv;
  },
  6: function(binary, byteLength) {
    // bitMap
    var rv = {};
    rv.indicator = tables.lookup(
      tables.tables.BitMapIndicator,
      binary.read('uint8')
    );
    if (rv.indicator == 0) {
      // has bitmap data.
      rv.data = 'NOT IMPLEMENTED. YOUR DATA IS HERE';
      // will look something like:
      ///rv.data = binary.read(['blob', byteLength-5-1]);
    }
    return rv;
  }
};

// Parser for scale factor / scaled value. See 92.1.12
var parseScaledValue = function(binary) {
  var scale = binary.read('uint8'), value = binary.read('uint32');
  return value * Math.pow(10, -scale);
}

// Parser for basic angle
var parseBasicAngle = function(binary) {
  var basicAngle = binary.read('uint32'), basicAngleSub = binary.read('uint32');

  basicAngle = ((basicAngle == 0) || (basicAngle == 0xffffffff)) ? 1 : basicAngle;
  basicAngleSub = ((basicAngleSub == 0) || (basicAngleSub == 0xffffffff)) ? 1e6 : basicAngleSub;

  return basicAngle / basicAngleSub;
}

var dataRepresentationTemplateParsers = {
  // Grid point data - simple packing
  0 : function(binary) {
    var rv = {}
    rv.name = "Grid point data - simple packing";
    rv.referenceValue = binary.read('float32');
    rv.binaryScaleFactor = binary.read('int16');
    rv.decimalScaleFactor = binary.read('int16');
    rv.numberOfBitsUsed = binary.read('uint8');
    rv.originalType = binary.read('uint8');
    return rv;
  }


}

var gridParsers = {
  // Grid Definition Template 3.0: Latitude/longitude (or equidistant cylindrical, or Plate Carree)
  0: function(binary) { var rv = {}
    rv.name = 'Latitude/longitude (or equidistant cylindrical, or Plate Carree)';
    rv.earthShape = tables.lookup(tables.tables.EarthShape, binary.read('uint8'));
    rv.sphericalRadius = parseScaledValue(binary);
    rv.majorAxis = parseScaledValue(binary);
    rv.minorAxis = parseScaledValue(binary);
    rv.ni = binary.read('uint32'); rv.nj = binary.read('uint32');
    rv.basicAngle = parseBasicAngle(binary);
    rv.la1 = binary.read('int32'); rv.lo1 = binary.read('int32');
    rv.resolutionAndComponentFlags = binary.read('uint8');
    rv.la2 = binary.read('int32'); rv.lo2 = binary.read('int32');
    rv.di = binary.read('int32'); rv.dj = binary.read('int32');
    rv.scanningMode = binary.read('uint8');

    var scale = rv.basicAngle;
    rv.la1 *= scale; rv.lo1 *= scale;
    rv.la2 *= scale; rv.lo2 *= scale;
    rv.di *= scale; rv.dj *= scale;

    return rv;
  },
  // Grid Definition Template 3.10: Mercator
  10: function(binary) { var rv={};
    rv.name = 'Mercator';
    rv.earthShape = tables.lookup(tables.tables.EarthShape, binary.read('uint8'));
    rv.sphericalRadius = parseScaledValue(binary);
    rv.majorAxis = parseScaledValue(binary);
    rv.minorAxis = parseScaledValue(binary);
    rv.ni = binary.read('uint32'); rv.nj = binary.read('int32');
    rv.la1 = binary.read('int32'); rv.lo1 = binary.read('int32');
    rv.resolutionAndComponentFlags = binary.read('uint8');
    rv.lad = binary.read('int32');
    rv.la2 = binary.read('int32'); rv.lo2 = binary.read('int32');
    rv.scanningMode = binary.read('uint8');
    rv.gridOrientation = binary.read('uint32');
    rv.di = binary.read('int32'); rv.dj = binary.read('int32');

    var scale = 1e-6;
    rv.la1 *= scale; rv.lo1 *= scale;
    rv.lad *= scale;
    rv.la2 *= scale; rv.lo2 *= scale;
    return rv;
  },
  // Grid Definition Template 3.20: Polar stereographic projection
  20: function(binary) { var rv={};
    rv.name = 'Polar stereographic projection';
    rv.earthShape = tables.lookup(tables.tables.EarthShape, binary.read('uint8'));
    rv.sphericalRadius = parseScaledValue(binary);
    rv.majorAxis = parseScaledValue(binary);
    rv.minorAxis = parseScaledValue(binary);
    rv.nx = binary.read('uint32'); rv.ny = binary.read('uint32');
    rv.la1 = binary.read('int32'); rv.lo1 = binary.read('int32');
    rv.resolutionAndComponentFlags = binary.read('uint8');
    rv.lad = binary.read('int32'); rv.lov = binary.read('int32');
    rv.dx = binary.read('int32'); rv.dy = binary.read('int32');
    rv.projectionCenter = binary.read('uint8');
    rv.scanningMode = binary.read('uint8');
    var scale = 1e-6;
    rv.la1 *= scale; rv.lo1 *= scale;
    rv.lad *= scale; rv.lov *= scale;
    return rv;
  },
  // Grid Definition Template 3.30: Lambert conformal
  30: function(binary) { var rv={};
    rv.name = 'Polar stereographic projection';
    rv.earthShape = tables.lookup(tables.tables.EarthShape, binary.read('uint8'));
    rv.sphericalRadius = parseScaledValue(binary);
    rv.majorAxis = parseScaledValue(binary);
    rv.minorAxis = parseScaledValue(binary);
    rv.nx = binary.read('uint32'); rv.ny = binary.read('uint32');
    rv.la1 = binary.read('int32'); rv.lo1 = binary.read('int32');
    rv.resolutionAndComponentFlags = binary.read('uint8');
    rv.lad = binary.read('int32'); rv.lov = binary.read('int32');
    rv.dx = binary.read('int32'); rv.dy = binary.read('int32');
    rv.projectionCenter = binary.read('uint8');
    rv.scanningMode = binary.read('uint8');
    rv.latin1 = binary.read('uint32');rv.latin2 = binary.read('uint32');
    rv.laSouthPole = binary.read('uint32');rv.loSouthPole = binary.read('uint32');
    var scale = 1e-6;
    rv.la1 *= scale; rv.lo1 *= scale;
    rv.lad *= scale; rv.lov *= scale;
    //rv.latin1 *= scale; rv.latin1 *= scale;
    //rv.laSouthPole *= scale; rv.loSouthPole *= scale;
    return rv;
  },
  // Grid Definition Template 3.40: Gaussian latitude/longitude
  40: function(binary) { var rv={};
    rv.name = 'Gaussian latitude/longitude';
    rv.earthShape = tables.lookup(tables.tables.EarthShape, binary.read('uint8'));
    rv.sphericalRadius = parseScaledValue(binary);
    rv.majorAxis = parseScaledValue(binary);
    rv.minorAxis = parseScaledValue(binary);
    rv.earthShape = binary.read('uint8');
    rv.ni = binary.read('uint32');rv.nj = binary.read('uint32');
    rv.basicAngle = binary.read('uint32');
    rv.la1 = binary.read('int32');rv.lo1 = binary.read('int32');
    rv.resolutionAndComponentFlags = binary.read('uint8');
    rv.la2 = binary.read('int32');rv.lo2 = binary.read('int32');
    rv.di = binary.read('int32');
    rv.n = binary.read('uint32');
    rv.scanningMode = binary.read('uint8');
    var basicAngle = ((rv.basicAngle == 0) || (rv.basicAngle == 0xffffffff)) ? 1 : rv.basicAngle;
    var scale = 1e-6/basicAngle;
    rv.la1 *= scale; rv.lo1 *= scale;
    rv.la2 *= scale; rv.lo2 *= scale;
    rv.di *= scale;
    return rv;
  },
  // Grid Definition Template 3.90: Space view perspective or orthographic
  // FIXME: implement properly
  90: function(binary) { var rv={};
    rv.name = 'Space view perspective or orthographic';
    rv.earthShape = tables.lookup(tables.tables.EarthShape, binary.read('uint8'));
    rv.sphericalRadius = parseScaledValue(binary);
    rv.majorAxis = parseScaledValue(binary);
    rv.minorAxis = parseScaledValue(binary);
    rv.nx = binary.read('uint32');rv.ny = binary.read('uint32');
    rv.basicAngle = binary.read('uint32');
    rv.lap = binary.read('int32');rv.lop = binary.read('int32');
    rv.resolutionAndComponentFlags = binary.read('uint8');
    rv.dx = binary.read('uint32');rv.dy = binary.read('uint32');
    rv.Xp = binary.read('uint32');rv.Yp = binary.read('uint32');
    rv.scanningMode = binary.read('uint8');
    var scale = 1e-6;
    rv.lap *= scale; rv.lop *= scale;
    return rv;
  },
};
