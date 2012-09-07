var net = require('net');
var _ = require('underscore');
var RequestTypes = require('./RequestTypes').RequestTypes;
var BufferMaker = require('buffermaker');
var binary = require('binary');

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
    var encodedMessage = that.encode(message);
    messageSetBufferMaker = messageSetBufferMaker.UInt32BE(encodedMessage.length).string(encodedMessage);
  });
  var messageSet = messageSetBufferMaker.make();


  var preMessageBuffer = new BufferMaker().UInt16BE(RequestTypes.PRODUCE)
  .UInt16BE(this.topic.length)
  .string(this.topic)
  .UInt32BE(this.partition).make();

  messages = Buffer.concat([new BufferMaker().UInt32BE(messageSet.length).make(), messageSet]);

  var data = Buffer.concat([preMessageBuffer, messages]);
  return Buffer.concat([new BufferMaker().UInt32BE(data.length).make(), data]);

};

exports.Producer = Producer;
