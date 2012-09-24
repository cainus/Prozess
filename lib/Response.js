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
  this.error = error;
  this.body = body;
  this.length = body.length + 4;  // body length + error field length
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
  var length = binary.parse(data).word32bu('length').vars.length;
  console.log("length was: ", length);
  var unpacked = binary.parse(data)
      .word32bu('length')
      .word16bs('error')
      .buffer('body', length)
      .vars;
      console.log("unpacked: ", unpacked);
  return new Response(unpacked.error, unpacked.body);
};


module.exports = Response;
