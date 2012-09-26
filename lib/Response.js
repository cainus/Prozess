var binary = require('binary');
var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var _ = require('underscore');

/*
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                        RESPONSE_LENGTH                        |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |         ERROR_CODE            |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  RESPONSE_LENGTH = int32 // Length in bytes of entire response (excluding this field)
  ERROR_CODE = int16 // See table below.

  ================  =====  ===================================================
  ERROR_CODE        VALUE  DEFINITION
  ================  =====  ===================================================
  Unknown            -1    Unknown Error
  NoError             0    Success
  OffsetOutOfRange    1    Offset requested is no longer available on the server
  InvalidMessage      2    A message you sent failed its checksum and is corrupt.
  WrongPartition      3    You tried to access a partition that doesn't exist
                           (was not between 0 and (num_partitions - 1)).
  InvalidFetchSize    4    The size you requested for fetching is smaller than
                           the message you're trying to fetch.
  ================  =====  ===================================================

 */
var Response = function(error, body){
  if (error < -1 || error > 4){ 
    throw "Invalid error code: " + error;
  }
  this.error = error;
  if (!Buffer.isBuffer(body)){
    throw "The body parameter must be a Buffer object.";
  }
  this.body = body;  // a Buffer object
  this.length = body.length + 2;  // body length + error field length
};

Response.Errors = {
  'Unknown' :         -1,  // Unknown Error
  'NoError' :          0,  // Success
  'OffsetOutOfRange' : 1,  //    Offset requested is no longer available on the server
  'InvalidMessage' :   2,  //    A message you sent failed its checksum and is corrupt.
  'WrongPartition' :   3,  //    You tried to access a partition that doesn't exist
                           //    (was not between 0 and (num_partitions - 1)).
  'InvalidFetchSize' : 4   //    The size you requested for fetching is smaller than
                           //    the message you're trying to fetch.

};

Response.fromBytes = function(data){
  if (!Buffer.isBuffer(data)){
    throw "The data parameter must be a Buffer object.";
  }
  var length = binary.parse(data).word32bu('length').vars.length;
  var unpacked = binary.parse(data)
      .word32bu('length')
      .word16bs('error')
      .buffer('body', length)
      .vars;
      if (unpacked.body.length + 4 < length){
        throw "incomplete response";
      }
  return new Response(unpacked.error, unpacked.body);
};

Response.prototype.toBytes = function(){
  return new BufferMaker()
               .UInt32BE(this.body.length + 2)  // + 2 bytes for the errorCode
               .UInt16BE(this.error)
               .string(this.body)
               .make();
};

module.exports = Response;
