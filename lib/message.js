var crc32 = require('crc32');
var binary = require('binary');

/*
  A message. The format of an N byte message is the following:
  1 byte "magic" identifier to allow format changes
  4 byte CRC32 of the payload
  N - 5 byte payload
*/

function Message(payload, magic, checksum){
  // note: payload should be a Buffer
  this.MAGIC_IDENTIFIER_DEFAULT = 0;
  this.magic = magic || this.MAGIC_IDENTIFIER_DEFAULT;
  this.payload = payload || new Buffer([]);
  this.checksum = checksum || this.calculateChecksum();
}

Message.prototype.calculateChecksum = function(){
  return parseInt(crc32(this.payload), 16);
};

Message.prototype.isValid = function(){
  console.log(this.checksum, ' =? ', this.calculateChecksum());
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
    .word32bu('checksum')
    .vars;

  var size = unpacked.size;
  var magic = unpacked.magic;
  var checksum = unpacked.checksum;
  var payload  = buf.slice(9, size + 1); // 5 = 1 + 4 is Magic + Checksum
  return new Message(payload, magic, checksum);
};

exports.Message = Message;

