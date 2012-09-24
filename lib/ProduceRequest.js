var binary = require('binary');
var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var Request = require('../lib/Request');
var _ = require('underscore');


var ProduceRequest = function(topic, partition, messages){
  this.topic = topic;
  this.partition = partition;
  this.messages = messages;
  this.requestType = 0; // produce
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
  var messagesLength = new BufferMaker().UInt32BE(messagesBuffer.length).make();
  var body = Buffer.concat([messagesLength, messagesBuffer]);
  var req = new Request(this.topic, this.partition, Request.Types.PRODUCE, body);
  return req.toBytes();
};


module.exports = ProduceRequest;

