var crc32 = require('crc32');
var binary = require('binary');
var BufferMaker = require('buffermaker');
var _ = require('underscore');
var BASIC_MESSAGE_HEADER_SIZE = 5;
var MESSAGE_SIZE_BYTE_LENGTH = 4;
var V0_7_MESSAGE_HEADER_SIZE = 10;
var V0_6_MESSAGE_HEADER_SIZE = 9;
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
  this.MAGIC_IDENTIFIER_DEFAULT = 1;
  if (magic === undefined){ 
    this.magic = this.MAGIC_IDENTIFIER_DEFAULT;
  } else { 
    this.magic = magic;
  }
  if (this.magic !== 0 && this.magic !== 1){
    throw "Unsupported Kafka message version: " + this.magic;
  }
  if (!payload && payload !== ''){
    throw "payload is a required argument";
  }
  this.payload = payload;
  this.checksum = checksum || this.calculateChecksum();
}

Message.prototype.byteLength = function(){
  if (!this.bytesLengthVal){
    this.toBytes();
  }
  return this.bytesLengthVal;
};

Message.prototype.calculateChecksum = function(){
  return parseInt(crc32(this.payload), 16);
};

Message.prototype.isValid = function(){
  return this.checksum == this.calculateChecksum();
};


/*
 
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                             LENGTH                            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |     MAGIC       |  COMPRESSION  |           CHECKSUM          |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |      CHECKSUM (cont.)           |           PAYLOAD           /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                             /
  /                         PAYLOAD (cont.)                       /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  LENGTH = int32 // Length in bytes of entire message (excluding this field)
  MAGIC = int8 // 0 = COMPRESSION attribute byte does not exist (v0.6 and below)
               // 1 = COMPRESSION attribute byte exists (v0.7 and above)
  COMPRESSION = int8 // 0 = none; 1 = gzip; 2 = snappy;
                     // Only exists at all if MAGIC == 1
  CHECKSUM = int32  // CRC32 checksum of the PAYLOAD
  PAYLOAD = Bytes[] // Message content

*/
Message.prototype.toBytes = function(){
  var nonLength = new BufferMaker()
    .UInt8(this.magic)
    .UInt8(this.compression)
    .UInt32BE(this.calculateChecksum())
    .string(this.payload)
    .make();
  var encodedMessage = new BufferMaker()
    .UInt32BE(nonLength.length)
    .string(nonLength)
    .make();
  this.bytesLengthVal = encodedMessage.length;
  return encodedMessage;

};

Message.fromBytes = function(buf){
  // Format is:
  //  32-bit unsigned Integer, network (big-endian) byte order
  //  8-bit unsigned Integer
  //  32-bit unsigned Integer, network (big-endian) byte order

  this.bytesLengthVal = buf.length;

  var unpacked = binary.parse(buf)
            .word32bu('size')
            .word8u('magic')
            .vars;

  var size = unpacked.size;
  var payloadSize;

  if (unpacked.magic === 0){
      payloadSize = size - 5;
      unpacked = binary.parse(buf)
        .word32bu('size')
        .word8u('magic')
        .word32bu('checksum')
        .buffer('payload', payloadSize)
        .vars;
  } else {
    // magic is assumed to be 1 here.
    // if it's not, it's invalid and will throw an error when we construct later
      payloadSize = size - 6;
      unpacked = binary.parse(buf)
        .word32bu('size')
        .word8u('magic')
        .word8u('compression')
        .word32bu('checksum')
        .buffer('payload', payloadSize)
        .vars;
  }

  var magic = unpacked.magic;
  var compression = unpacked.compression;
  var checksum = unpacked.checksum;
  var payload  = unpacked.payload;

  if (payload.length < payloadSize){
    throw "incomplete message";
  }

  return new Message(payload, checksum, magic, compression);
};

module.exports = Message;

