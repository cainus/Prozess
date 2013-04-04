var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var Request = require('../lib/Request');
var _ = require('underscore');
var binary = require('binary');
var assert = require('assert');

var ProduceRequest = function(topic, partition, messages){
  this.topic = topic;
  this.partition = partition;
  this.messages = messages;
  this.requestType = Request.Types.PRODUCE;
};

/*

 0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                         REQUEST HEADER                        /
  /                                                               /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         MESSAGES_LENGTH                       |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                                                               /
  /                            MESSAGES                           /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  MESSAGES_LENGTH = int32 // Length in bytes of the MESSAGES section
  MESSAGES = Collection of MESSAGES (see above)

*/
ProduceRequest.prototype.toBytes = function(){
  var messages = this.messages;
  var that = this;
  var messageBuffers = [];
  _.each(messages, function(message){
    var bytes = message.toBytes();
    messageBuffers.push(bytes);
  });
  var messagesBuffer = Buffer.concat(messageBuffers);
  assert(Buffer.isBuffer(messagesBuffer));
  var messagesLengthBuffer = new BufferMaker().UInt32BE(messagesBuffer.length).make();
  var body = Buffer.concat([messagesLengthBuffer, messagesBuffer]);
  var req = new Request(this.topic, this.partition, Request.Types.PRODUCE, body);
  return req.toBytes();
};

ProduceRequest.fromBytes = function(buffer) {
  var request = Request.fromBytes(buffer);
  var messages = toMessages(getMessagesSegment(request.body));
  return new ProduceRequest(request.topic, request.partition, messages);
};

module.exports = ProduceRequest;

var MESSAGES_LENGTH_SIZE = 4;

var getLengthOfMessagesSegment = function(buffer) {
   assert(!_.isString(binary.parse(buffer).word32bu('length').vars));
   return binary.parse(buffer).word32bu('length').vars.length;
};
var toMessages = function(messagesBuffer) {
  var messages = [];
  while(messagesBuffer.length > 0) {
    var message = Message.fromBytes(messagesBuffer);
    messages.push(message);
    messagesBuffer = messagesBuffer.slice(message.toBytes().length);
  }
  return messages;
};
var getMessagesSegment = function(body) {
  var start = MESSAGES_LENGTH_SIZE;
  var stop =  getLengthOfMessagesSegment(body) + MESSAGES_LENGTH_SIZE;
  return body.slice(start, stop);
};
