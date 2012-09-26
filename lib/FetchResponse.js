var binary = require('binary');
var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var Response = require('../lib/Response');
var _ = require('underscore');

/*  HEADER

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
/*
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                          RESPONSE HEADER                      /
  /                                                               /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                        MESSAGES (0 or more)                   /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*/
var FetchResponse = function(error, messages){
  var that = this;
  this.error = error;
  this.messages = messages;  // an array of message objects
  this.length = 2;
  _.each(this.messages, function(msg){
    that.length = msg.toBytes().length;
  });
  this.bytesLengthVal = null;
};

FetchResponse.prototype.byteLength = function(){
  if (!this.bytesLengthVal){
    this.toBytes();
  }
  return this.bytesLengthVal;
};

FetchResponse.prototype.toBytes = function(){
  var body = new BufferMaker();
  _.each(this.messages, function(msg){
    body.string(msg.toBytes());
  });
  body = body.make();
  var bytes = new BufferMaker()
                .UInt32BE(body.length + 2)
                .UInt16BE(this.error)
                .string(body)
                .make();
  this.bytesLengthVal = bytes.length;
  return bytes;
};


FetchResponse.fromBytes = function(bytes){
  var response = Response.fromBytes(bytes);
  var messages;
  if (response.error !== Response.Errors.NoError){
    messages = [];
  } else {
    messages = parseMessages(response.body);
  }
  this.bytesLengthVal = response.toBytes().length;

  return new FetchResponse(response.error, messages);
};

module.exports = FetchResponse;


var parseMessages = function(body){
  // TODO: parse bodies with more than one message, or no message, or a partial message.
  var messages = [];
  while(body.length > 0){
    try {
      var message = Message.fromBytes(body);
      body = body.slice(message.toBytes().length);
      messages.push(message);
    } catch(ex){
      break;
    }
  }
  return messages;
};
