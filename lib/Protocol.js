
var BufferMaker = require('buffermaker');
var _ = require('underscore');


/*  This class abstracts away all byte-handling, so there should be no byte handling 
 *  elsewhere in the lib other than here and in any message-specific classes that this
 *  class uses.
 */

var Protocol = function(topic, partition, maxMessageSize){
  this.topic = topic;
  this.partition = partition;
  this.maxMessageSize = maxMessageSize;
  this.RequestTypes = {
    PRODUCE      : 0,
    FETCH        : 1,
    MULTIFETCH   : 2,
    MULTIPRODUCE : 3,
    OFFSETS      : 4
  };
};


Protocol.prototype.encodeFetchRequest = function(offset){
  var body = new BufferMaker()
              .Int64BE(offset)
              .UInt32BE(this.maxMessageSize)
              .make();
  var header = encodeRequestHeader(this.RequestTypes.FETCH, this.topic, this.partition, body.length);
  return Buffer.concat([header, body]);
              /*
      return new BufferMaker()
              .UInt16BE(this.RequestTypes.FETCH)
              .UInt16BE(this.topic.length)
              .string(this.topic)
              .UInt32BE(this.partition)
              .Int64BE(offset)
              .UInt32BE(this.maxMessageSize)
              .make();
             */
};

Protocol.prototype.encodeOffsetsRequest = function(time, maxOffsets){
  if (time === -1){
    time = new Buffer([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
  } else {
    throw "TODO: support unix timestamp and -2";
  }
  var requestBody = new BufferMaker()
                        .string(time)
                        .UInt32BE(maxOffsets)
                        .make();
                        // 12 for the body
  var requestBodyLength = requestBody.length;
  var requestType = this.RequestTypes.OFFSETS;
  var topic = this.topic;
  var partition = this.partition;
  var header = encodeRequestHeader(requestType, topic, partition, requestBodyLength);
  return Buffer.concat([header, requestBody]);
};



Protocol.prototype.encodeMessage = function(message){
  var encodedMessage = new BufferMaker()
  .UInt8(message.magic)
  .UInt8(message.compression)
  .UInt32BE(message.calculateChecksum())
  .string(message.payload)
  .make();

  return encodedMessage;

};



Protocol.prototype.encodeProduceRequest = function(messages){
  var that = this;
  var messageSetBufferMaker = new BufferMaker();
  _.each(messages, function(message){
    var encodedMessage = that.encodeMessage(message);
    messageSetBufferMaker
                  .UInt32BE(encodedMessage.length)
                  .string(encodedMessage);
  });
  var messageSet = messageSetBufferMaker.make();
  var messagesBuffer = Buffer.concat([new BufferMaker().UInt32BE(messageSet.length).make(), messageSet]);
  var preMessageBuffer = encodeRequestHeader(this.RequestTypes.PRODUCE, this.topic, this.partition, messagesBuffer.length);
  var encodedRequest = Buffer.concat([preMessageBuffer, messagesBuffer]);
  return encodedRequest;
};

module.exports = Protocol;


var encodeRequestHeader = function(requestType, topic, partition, requestBodyLength){
  var requestHeader = new BufferMaker()
  /* request length = request type + topic length's length + topic length + partition + body length */
  .UInt32BE(2 + 2 + topic.length + 4 + requestBodyLength) 
  .UInt16BE(requestType)
  .UInt16BE(topic.length)
  .string(topic)
  .UInt32BE(partition)
  .make();
  return requestHeader;

};
