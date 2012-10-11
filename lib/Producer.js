var net = require('net');
var _ = require('underscore');
var BufferMaker = require('buffermaker');
var binary = require('binary');
var Message = require('./Message');
var ProduceRequest = require('./ProduceRequest');

var Producer = function(topic, options){
  if (!topic || (!_.isString(topic))){
    throw "the first parameter, topic, is mandatory.";
  }
  this.MAX_MESSAGE_SIZE = 1024 * 1024; // 1 megabyte
  options = options || {};
  this.topic = topic;
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;
  this.errorHandler = function() {};
};


Producer.prototype.connect = function(cb){
  var that = this;
  this.connection = net.createConnection({
    port : this.port,
    host : this.host
  });
  this.connection.on('connect', cb);
  this.connection.on('error', function(err){
    that.errorHandler(err);
  });
};

Producer.prototype.send = function(messages, cb) {
  // try to make messages if we didn't get them
  if (!_.isArray(messages)){
    messages = [messages];
  }
  messages = _.map(messages, function(msg){
    if (msg instanceof Message){
      return msg;
    }
    return new Message(msg);
  });
  var request = new ProduceRequest(this.topic, this.partition, messages);
  this.connection.write(request.toBytes(), cb);
};

Producer.prototype.on = function(event, cb) {
  if (event === 'error') {
    this.errorHandler = cb;
  }
};

module.exports = Producer;
