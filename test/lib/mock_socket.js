var util = require('util');
var EventEmitter = require('events').EventEmitter;
var net = require('net');

var MockSocket = function(opt) {
  EventEmitter.call(this);
  this.connected = false;
};

util.inherits(MockSocket, EventEmitter);

exports.socketBehavior = {};

function handleBehavior(behavior, type, succ, fail) {
  function handle(behaviorType) {
    if (behaviorType.type === 'error') {
      fail();
    } else if (behaviorType.type === 'wait') {
      behaviorType.wait(function(err) {
        if (err) {
            fail();
        } else {
            succ();
        }
      });
    } else if (behaviorType.type === 'series') {
      var sub = behaviorType.series.shift();
      if (sub) {
        handle(sub);
      } else {
        succ();
      }
    } else {
      succ();
    }
  }
  if (behavior && behavior[type]) {
    var behaviorType = behavior[type];
    if (behaviorType.single) {
      delete behavior[type];
    }
    handle(behaviorType);
  } else {
    succ();
  }
}

MockSocket.prototype.connect = function(port, host, listener) {
  if (!listener && typeof host === 'function') {
    listener = host;
    host = undefined;
  }
  if (listener) {
    this.on('connect', listener);
  }
  if (!host) {
    host = 'localhost';
  }
  var path;
  if (typeof port !== 'number') {
    path = port;
    port = undefined;
    this.key = path;
  } else {
    this.key = host + ":" + port;
  }
  var behavior = exports.socketBehavior[this.key];

  function fail() {
    that.emit('error', new Error('connect ECONNREFUSED'));
  }
  function succeed() {
    that.connected = true;
    that.emit('connect');
  }

  var that = this;
  setTimeout(function() {
    handleBehavior(behavior, 'connect', succeed, fail);
  }, 10);
};

MockSocket.prototype.setEncoding = function() {};

MockSocket.prototype.write = MockSocket.prototype._write = function(data, encoding, callback) {
  var that = this;
  if (!callback && typeof encoding === 'function') {
    callback = encoding;
    encoding = undefined;
  }

  function fail() {
    var err = new Error('This socket is closed.');
    that.emit('error', err);
    if (callback) {
      callback(err);
    }
  }

  if (!this.connected) {
    return fail();
  }
  var behavior = exports.socketBehavior[this.key];
  handleBehavior(behavior, 'write', callback, fail);
  return true;
};

MockSocket.prototype._read = function(n) {
};

MockSocket.prototype.end = function(data, encoding) {
  this.write(data, encoding);
  this.connected = false;
};

MockSocket.prototype.destroy = function() {
  this.connected = false;
};

MockSocket.prototype.pause = function() {
};

MockSocket.prototype.resume = function() {
};

MockSocket.prototype.setTimeout = function(timeout, callback) {
  if (callback) {
    this.once('timeout', callback);
  }
};

MockSocket.prototype.setNoDelay = function() {
};

MockSocket.prototype.setKeepAlive = function() {
};

MockSocket.prototype.address = function() {
  return {};
};

MockSocket.prototype.unref = function() {};
MockSocket.prototype.ref = function() {};

var oldSocket = net.Socket;
var oldConnect = net.createConnection;

exports.install = function() {
  net.Socket = MockSocket;
  net.createConnection = function(port, host, listener) {
    var sock = new MockSocket({});
    sock.connect(port, host, listener);
    return sock;
  };
};

exports.restore = function() {
  net.Socket = oldSocket;
  net.createConnection = oldConnect;
};
