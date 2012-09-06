var net = require('net');
var _ = require('underscore');
var RequestTypes = require('./requesttypes').RequestTypes;
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

exports.Producer = Producer;
