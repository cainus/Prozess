var crc32 = require('crc32');
var binary = require('binary');
var _ = require('underscore');

/*
  A message. The format of an N byte message is the following:
  1 byte "magic" identifier to allow format changes
  4 byte CRC32 of the payload
  N - 5 byte payload
*/

function Message(payload, checksum, magic, compression){
  // note: payload should be a Buffer
  this.COMPRESSION_DEFAULT = 0;
  this.compression = this.COMPRESSION_DEFAULT;

  // compression:
  // 0 = none; 1 = gzip; 2 = snappy;
  // Only exists at all if MAGIC == 1
  if (!!magic && magic == 1){
    if (_.include([0,1,2], compression)){
      this.compression = compression;
    } else {
      throw "InvalidCompressionType: " + compression;
    }
  }

  this.MAGIC_IDENTIFIER_DEFAULT = 0;
  this.magic = magic || this.MAGIC_IDENTIFIER_DEFAULT;
  this.payload = payload || new Buffer([]);
  this.checksum = checksum || this.calculateChecksum();
}

Message.prototype.calculateChecksum = function(){
  return parseInt(crc32(this.payload), 16);
};

Message.prototype.isValid = function(){
  return this.checksum == this.calculateChecksum();
};

Message.parseFrom = function(buf){
  // Format is:
  //  32-bit unsigned Integer, network (big-endian) byte order
  //  8-bit unsigned Integer
  //  32-bit unsigned Integer, network (big-endian) byte order

  var unpacked = binary.parse(buf)
    .word32bu('size')
    .word8u('magic')
    .word8u('compression')
    .word32bu('checksum')
    .vars;

  var size = unpacked.size;
  var magic = unpacked.magic;
  var compression = unpacked.compression;
  var checksum = unpacked.checksum;
  var payload  = buf.slice(10, size);
  return new Message(payload, checksum, magic, compression);
};
exports.Message = Message;

