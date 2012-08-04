var binary = require('binary');
var Message = require('../lib/message').Message;

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

var FetchResponse = function(length, error, body){
  this.error = error;
  this.length = length;
  console.log("body :", body);
  this.messages = this.parseMessages(body);
};

FetchResponse.prototype.parseMessages = function(body){
  // TODO: parse bodies with more than one message, or no message, or a partial message.
  var messages = [];
  while(body.length > 0){
    console.log("LOOP!");
    var message = Message.parseFrom(body);
    messages.push(message);
    body = body.slice(message.length);
    console.log("body: ", body);
  }
  return messages;
};

FetchResponse.parseFrom = function(bytes){
  var vars = binary.parse(bytes)
      .word32bu('length')
      .word16bs('error')
      .vars;

  var expectedLength = vars.length;

  vars = binary.parse(bytes)
      .word32bu('length')
      .word16bs('error')
      .buffer('body', expectedLength)
      .vars;

  return new FetchResponse(vars.length, vars.error, vars.body);
  
};


exports.FetchResponse = FetchResponse;

