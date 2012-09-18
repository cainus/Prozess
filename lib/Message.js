var crc32 = require('crc32');
var binary = require('binary');
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
  this.payload = payload || '';
  this.checksum = checksum || this.calculateChecksum();
}

Message.prototype.calculateChecksum = function(){
  return parseInt(crc32(this.payload), 16);
};

Message.prototype.isValid = function(){
  return this.checksum == this.calculateChecksum();
};

Message.parseFrom = function(buf, cb){
  // Format is:
  //  32-bit unsigned Integer, network (big-endian) byte order
  //  8-bit unsigned Integer
  //  32-bit unsigned Integer, network (big-endian) byte order

  var messageSet = { size : 0, messages : [] };
  var bytesProcessed = 0;
  var bytesTotal = buf.length - BASIC_MESSAGE_HEADER_SIZE;
  while (bytesProcessed <= bytesTotal ) { 
    var remaining_header = buf.slice(bytesProcessed, bytesProcessed + BASIC_MESSAGE_HEADER_SIZE);
    var unpacked = binary.parse(remaining_header)
    .word32bu('size')
    .word8u('magic')
    .vars;

    if ((bytesProcessed + unpacked.size + MESSAGE_SIZE_BYTE_LENGTH) > buf.length) {
      // message is truncated
      break;
    }

    var payloadIndex; 
    var payloadSize;
    var remaining;

    switch (unpacked.magic)
    {
      case 0: 
        remaining = buf.slice(bytesProcessed);
      unpacked = binary.parse(remaining)
      .word32bu('size')
      .word8u('magic')
      .word32bu('checksum')
      .vars;
      payloadIndex = bytesProcessed + V0_6_MESSAGE_HEADER_SIZE;
      payloadSize = payloadIndex + unpacked.size - 5; // 5 = sizeof(magic) + sizeof(checksum)
      break;

      case 1:
        remaining = buf.slice(bytesProcessed);
      unpacked = binary.parse(remaining)
      .word32bu('size')
      .word8u('magic')
      .word8u('compression')
      .word32bu('checksum')
      .vars;
      payloadIndex = bytesProcessed + V0_7_MESSAGE_HEADER_SIZE;  // compression byte will exist if magic > 0
      payloadSize = payloadIndex + unpacked.size - 6; //6 = sizeof(magic) + sizeof(attrs) + sizeof(checksum)
      break;

      default: 
        cb("Unsupported Kafka message version");
    }

    var messageSize = unpacked.size;
    var magic = unpacked.magic;
    var compression = unpacked.compression;
    var checksum = unpacked.checksum;
    var payload  = buf.slice(payloadIndex,payloadSize);
    var size = messageSize + 4; // size of the message plus messageSize
    bytesProcessed += size;
  console.log(unpacked.magic);
    messageSet.messages.push(new Message(payload, checksum, magic, compression));
    messageSet.size += bytesProcessed;
  }
  return messageSet;
};
module.exports = Message;

