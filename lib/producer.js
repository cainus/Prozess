var net = require('net');
var _ = require('underscore');
var BufferMaker = require('buffermaker');
var binary = require('binary');
var Protocol = require('./Protocol');
var Message = require('./Message');

var Producer = function(options){
  this.MAX_MESSAGE_SIZE = 1024 * 1024; // 1 megabyte
  options = options || {};
  this.topic = options.topic || 'test';
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;
  this.protocol = new Protocol(this.topic, this.partition, this.MAX_MESSAGE_SIZE);
};


Producer.prototype.connect = function(cb){
  this.connection = net.createConnection( this.port, this.host, cb);
};

Producer.prototype.send = function(messages) {
  // try to make messages if we didn't get them
  messages = _.map(messages, function(msg){
    console.log(msg);
    console.log(Message);
    if (msg instanceof Message.constructor){
      return msg;
    }
    return new Message(msg);
  });
 this.connection.write(this.protocol.encodeProduceRequest(messages));
 this.connection.on('error', function(err){
   console.log(err);
 });
};

module.exports = Producer;
