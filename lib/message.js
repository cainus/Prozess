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

  this.MAGIC_IDENTIFIER_DEFAULT = 0;
  this.magic = magic || this.MAGIC_IDENTIFIER_DEFAULT;
  this.payload = payload || new Buffer([]);
  this.checksum = checksum || this.calculateChecksum();
}

Message.prototype.calculateChecksum = function(){
  return parseInt(crc32(this.payload), 16);
};

Message.prototype.isValid = function(){
  console.log("chcksum!!!");
  console.log(this.payload.toString());
  console.log(this.calculateChecksum());
  return this.checksum == this.calculateChecksum();
};

Message.parseFrom = function(buf){
  // Format is:
  //  32-bit unsigned Integer, network (big-endian) byte order
  //  8-bit unsigned Integer
  //  32-bit unsigned Integer, network (big-endian) byte order

  var messageSet = { size : 0, messages : [] };
  var bytesProcessed = 0;
  var bytesTotal = buf.length - BASIC_MESSAGE_HEADER_SIZE;
  while (bytesProcessed <= bytesTotal ) { 
    var remaining = buf.slice(bytesProcessed, bytesProcessed + BASIC_MESSAGE_HEADER_SIZE);
    console.log(remaining.length);
    var unpacked = binary.parse(remaining)
    .word32bu('size')
    .word8u('magic')
    .vars;

    if ((bytesProcessed + unpacked.size + MESSAGE_SIZE_BYTE_LENGTH) > buf.length) {
      // message is truncated
      console.log(bytesProcessed);
      console.log(unpacked.size);
      console.log(MESSAGE_SIZE_BYTE_LENGTH);
      console.log(buf.length);
      console.log("get out of loop");
      break;
    }

    var payloadIndex; 
    var payloadSize;
  console.log(unpacked);
    switch (unpacked.magic)
    {
      case 0: 
        unpacked = binary.parse(buf)
      .word32bu('size')
      .word8u('magic')
      .word32bu('checksum')
      .vars;
      payloadIndex = bytesProcessed + V0_6_MESSAGE_HEADER_SIZE;
      payloadSize = payloadIndex + unpacked.size - 5; // 5 = sizeof(magic) + sizeof(checksum)
      console.log("LOK OK HERE");
      console.log(unpacked.size);
      break;

      case 1:
        unpacked = binary.parse(buf)
      .word32bu('size')
      .word8u('magic')
      .word8u('compression')
      .word32bu('checksum')
      .vars;
      payloadIndex = bytesProcessed + V0_7_MESSAGE_HEADER_SIZE;  // compression byte will exist if magic > 0
      payloadSize = payloadIndex + unpacked.size - 6; //6 = sizeof(magic) + sizeof(attrs) + sizeof(checksum)
      break;

      default: 
        throw new Error("Unsupported Kafka message version");
    }

    var messageSize = unpacked.size;
    var magic = unpacked.magic;
    var compression = unpacked.compression;
    var checksum = unpacked.checksum;
    console.log("LOOK OK!");
    console.log(payloadIndex);
    console.log(payloadSize);
    var payload  = buf.slice(payloadIndex,payloadSize);
    var size = messageSize + 4; // size of the message plus messageSize
    bytesProcessed += size;

    console.log("BYTES!222222222222222222222222222!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    console.log(bytesProcessed);

   // console.log("unpacked: ", unpacked);
   // console.log("magic: ", magic);
   // console.log("compression: ", compression);
   // console.log("BUF: ", buf);
   // console.log("PLI: ", payloadPad);
   // console.log("to:  ", size + 4);
   // console.log("payload: ", payload);
    messageSet.messages.push(new Message(payload, checksum, magic, compression));
    messageSet.size += bytesProcessed;
  }
  return messageSet;
};
exports.Message = Message;

