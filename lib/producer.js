var net = require('net');
var _ = require('underscore');
var RequestTypes = require('./requesttypes');
var BufferMaker = require('buffermaker');
var binary = require('binary');
var Client = require('./client');

var Producer = function(options){
  options = options || {};
  this.requestType = RequestTypes.PRODUCE;
  this.topic = options.topic || 'test';
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;
};

Producer.prototype.encode = function(message){
  var encodedMessage = new BufferMaker()
  .UInt8(message.magic)
  .UInt8(message.compression)
  .UInt32BE(message.calculateChecksum())
  .string(message.payload)
  .make();

  return encodedMessage;

};



Producer.prototype.encodeRequest = function(messages){
  var that = this;
  var messageSetBufferMaker = new BufferMaker();
  _.each(messages, function(message){
    console.log("loop");
    var encodedMessage = that.encode(message);
    console.log("encodedMessage", encodedMessage);
    messageSetBufferMaker
                  .UInt32BE(encodedMessage.length)
                  .string(encodedMessage);
  });
  var messageSet = messageSetBufferMaker.make();
  var messagesBuffer = Buffer.concat([new BufferMaker().UInt32BE(messageSet.length).make(), messageSet]);
  var preMessageBuffer = Client.encodeRequestHeader(RequestTypes.PRODUCE, this.topic, this.partition, messagesBuffer.length);
  var encodedRequest = Buffer.concat([preMessageBuffer, messagesBuffer]);
  return encodedRequest;
};

Producer.prototype.connect = function(port, host, listener){
  this.connection = net.createConnection( port, host, listener);

};

Producer.prototype.send = function(messages) {
 this.connection.write(this.encodeRequest(messages));
 this.connection.on('error', function(err){
   console.log(err);
 });
};

exports.Producer = Producer;
