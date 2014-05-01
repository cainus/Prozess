var Writable = require('readable-stream').Writable;
var net = require('net');

var Connection = function(port, host) {
  Writable.call(this, {objectMode: true});
  this.port = port;
  this.host = host;

  this.state = Connection.DISCONNECTED;
  this._connection = null;
};

Connection.DISCONNECTED = 0;
Connection.CONNECTING = 1;
Connection.CONNECTED = 2;

Connection.prototype = Object.create(Writable.prototype);

Connection.prototype.connect = function() {
  var that = this;

  if (this.state === Connection.CONNECTED) {
    this.emit('connect');
    return;
  } else if (this.state === Connection.CONNECTING) {
    return;
  }

  this.state = Connection.CONNECTING;

  this._connection = net.createConnection(this.port, this.host);
  this._connection.setKeepAlive(true, 1000);

  this._connection.once('connect', function() {
    that.state = Connection.CONNECTED;
    that.emit('connect');
  });
  this._connection.on('error', function(err) {
      that.state = Connection.DISCONNECTED;
      that.emit('error', err);
  });
  this._connection.on('close', function() {
      that.state = Connection.DISCONNECTED;
      that.emit('close');
  });
};

Connection.prototype._reconnect = function(callback) {
  var that = this;

  if (this.state === Connection.CONNECTED) {
    return callback();
  }

  var onConnect = function() {
    that.removeListener('brokerReconnectError', onReconnectError);
    that._connection.removeListener('error', onConnectError);
    callback();
  };

  var onReconnectError = function() {
    that.removeListener('connect', onConnect);
    callback('brokerReconnectError');
  };

  var onConnectError = function(err) {
    if (!!err.message && err.message === 'connect ECONNREFUSED') {
      that.emit('brokerReconnectError', err);
    } else {
      callback(err);
    }
  };

  this.once('connect', onConnect);
  this.once('brokerReconnectError', onReconnectError);

  if (this.state === Connection.CONNECTING) return;

  this.connect();

  this._connection.once('error', onConnectError);
};

Connection.prototype._write = function(data, encoding, callback) {
  var that = this;

  this._connection.write(data, function(err) {
    if (!!err && err.message === 'This socket is closed.') {
      that._reconnect(function(err) {
        if (err) {
          return callback(err);
        }

        that._connection.write(data, function(err) {
          return callback(err);
        });
      });
    } else {
      callback(err);
    }
  });
};

module.exports = Connection;
