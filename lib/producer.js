var net = require('net');
var _ = require('underscore');
var RequestTypes = require('./RequestTypes');
var BufferMaker = require('buffermaker');
var binary = require('binary');
var Protocol = require('./Protocol');

var Producer = function(options){
  this.MAX_MESSAGE_SIZE = 1024 * 1024; // 1 megabyte
  options = options || {};
  this.requestType = RequestTypes.PRODUCE;
  this.topic = options.topic || 'test';
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;
  this.protocol = new Protocol(this.topic, this.partition, this.MAX_MESSAGE_SIZE);
};


Producer.prototype.connect = function(port, host, listener){
  this.connection = net.createConnection( port, host, listener);

};

Producer.prototype.send = function(messages) {
 this.connection.write(this.encodeProduceRequest(messages));
 this.connection.on('error', function(err){
   console.log(err);
 });
};

exports.Producer = Producer;
