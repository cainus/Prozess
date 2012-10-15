var net = require('net');
var _ = require('underscore');
var BufferMaker = require('buffermaker');
var binary = require('binary');
var EventEmitter = require('events').EventEmitter;
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
};

Producer.prototype = Object.create(EventEmitter.prototype);


Producer.prototype.connect = function(cb){
  var that = this;
  this.connection = net.createConnection({
    port : this.port,
    host : this.host
  });
  this.connection.on('connect', cb);
  this.connection.on('error', function(err){
    that.emit('error', err);
  });
};

Producer.prototype.send = function(messages, cb) {
  var that = this;
  if (!cb){
    cb = function(err){
        if (!!err){
            that.emit('error', err);
        }
    };
  }
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
  this.connection.write(request.toBytes(), function(err) {
    if (!!err && err.message === 'This socket is closed.') {
      that.connect(function(err) {
        if (!!err) {
          cb(err);
        } else {
          that.connection.write(request.toBytes(), function(err) {
            cb(err);
          });
        }
      });
    } else {
      cb(err);
    }
  });
};

module.exports = Producer;
