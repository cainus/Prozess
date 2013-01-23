var net = require('net');
var EventEmitter = require('events').EventEmitter;
var _ = require('underscore');
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

  this.connection = null;
  this.connecting = false;
};

Producer.prototype = Object.create(EventEmitter.prototype);

Producer.prototype.connect = function(){
  var that = this;
  this.connecting = true;
  this.connection = net.createConnection(this.port, this.host);
  this.connection.setKeepAlive(true, 1000);
  this.connection.once('connect', function(){
    that.connecting = false;
    that.emit('connect');
  });
  this.connection.once('error', function(err){
    if (!!err.message && err.message === 'connect ECONNREFUSED'){
      that.emit('error', err);
      that.connecting = false;
    }
  });
};

Producer.prototype._reconnect = function(cb){
  var producer = this;

  var onConnect = function(){
    producer.removeListener('brokerReconnectError', onBrokerReconnectError);
    return cb();
  };
  producer.once('connect', onConnect);

  var onBrokerReconnectError = function(err){
    producer.removeListener('connect', onConnect);
    return cb('brokerReconnectError');
  };
  producer.once('brokerReconnectError', onBrokerReconnectError);

  if (!producer.connecting){
    producer.connect();
    producer.connection.on('error', function(err){
      if (!!err.message && err.message === 'connect ECONNREFUSED'){
        producer.emit("brokerReconnectError", err);
      } else {
        // don't care about other errors that may fire here
      }
    });
  } else {
    // reconnect already in progress.  wait.
  }
};


Producer.prototype.send = function(messages, cb) {
  var that = this;
  if (!cb){
    throw "A callback with an error parameter must be supplied";
  }
  messages = toListOfMessages(toArray(messages));
  var request = new ProduceRequest(this.topic, this.partition, messages);
  this.connection.write(request.toBytes(), function(err) {
    if (!!err && err.message === 'This socket is closed.') {
      that._reconnect(function(err){
        if (err){
          return cb(err);
        }
        that.connection.write(request.toBytes(), function(err) {
          return cb(err);
        });
      });
    } else {
      cb(err);
    }
  });
};

module.exports = Producer;

var toListOfMessages = function(args) {
  return _.map(args, function(arg) {
    if (arg instanceof Message) {
      return arg;
    }
    return new Message(arg);
  });
};

var toArray = function(arg) {
  if (_.isArray(arg))
    return arg;
  return [arg];
};
