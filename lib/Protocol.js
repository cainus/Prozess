
var BufferMaker = require('BufferMaker');
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


Protocol.prototype.encodedRequestSize = function(){
  console.log("Protocol.encodedRequestSize topic: ", this.topic);
  var size = 2 + 2 + this.topic.length + 4 + 8 + 4;
  //[size].pack("N")
  console.log("size: ", size);
  var buffer = new Buffer(4);
  buffer.writeUInt32BE(size, 0);
  return buffer;

};


Protocol.prototype.encodeFetchRequest = function(offset){
  return this.encodeRequest( this.RequestTypes.FETCH, offset, this.maxMessageSize);
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
  console.log('12?', requestBodyLength);
  var requestType = this.RequestTypes.OFFSETS;
  var topic = this.topic;
  var partition = this.partition;
  console.log('encodeRequestHeader');
  console.log(requestType, topic, partition, requestBodyLength);
  var header = this.encodeRequestHeader(requestType, topic, partition, requestBodyLength);
  console.log('headerlength: ', header.length);
  return Buffer.concat([header, requestBody]);
};


// TODO kill this?  seems to only support fetch
Protocol.prototype.encodeRequest = function( request_type, offset, maxMessageSize){

      return new BufferMaker()
              .UInt16BE(request_type)
              .UInt16BE(this.topic.length)
              .string(this.topic)
              .UInt32BE(this.partition)
              .Int64BE(offset)
              .UInt32BE(maxMessageSize)
              .make();
      //offset       = [offset].pack("q").reverse # DIY 64bit big endian integer
};


Protocol.prototype.encodeRequestHeader = function(requestType, topic, partition, requestBodyLength){
  console.log("partition: ", partition);
  var requestHeader = new BufferMaker()
  /* request length = request type + topic length's length + topic length + partition + body length */
  .UInt32BE(2 + 2 + topic.length + 4 + requestBodyLength) 
  .UInt16BE(requestType)
  .UInt16BE(topic.length)
  .string(topic)
  .UInt32BE(partition)
  .make();
  console.log("returning request header: ", requestHeader);
  return requestHeader;
  

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
    console.log("loop");
    var encodedMessage = that.encodeMessage(message);
    console.log("encodedMessage", encodedMessage);
    messageSetBufferMaker
                  .UInt32BE(encodedMessage.length)
                  .string(encodedMessage);
  });
  var messageSet = messageSetBufferMaker.make();
  var messagesBuffer = Buffer.concat([new BufferMaker().UInt32BE(messageSet.length).make(), messageSet]);
  var preMessageBuffer = this.encodeRequestHeader(this.RequestTypes.PRODUCE, this.topic, this.partition, messagesBuffer.length);
  var encodedRequest = Buffer.concat([preMessageBuffer, messagesBuffer]);
  return encodedRequest;
};

module.exports = Protocol;
