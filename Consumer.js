var net = require('net');
var _ = require('underscore');
var Message = require('./Message');
var BufferMaker = require('buffermaker');
var bignum = require('bignum');
var OffsetsResponse = require('./OffsetsResponse');
var FetchRequest = require('./FetchRequest');
var OffsetsRequest = require('./OffsetsRequest');
var FetchResponse = require('./FetchResponse');
var Response = require('./Response');

var Consumer = function(options){
  this.MAX_MESSAGE_SIZE = 1024 * 1024; // 1 megabyte
  this.MESSAGE_HEADER_LENGTH = Message.getHeaderLength();
  this.DEFAULT_POLLING_INTERVAL = 2; // 2 seconds
  this.MAX_OFFSETS = 1;
  this.LATEST_OFFSET = -1;
  this.EARLIEST_OFFSET = -2;

  options = options || {};
  this.topic = options.topic || 'test';
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;
  this.offset = (typeof(options.offset) != "undefined") ? bignum(options.offset) : null;
  this.maxMessageSize = options.maxMessageSize || this.MAX_MESSAGE_SIZE;
  this.polling = options.polling  || this.DEFAULT_POLLING_INTERVAL;
  this.unprocessedData = new Buffer([]);

  this.requestMode = null;
  this.responseBuffer = new Buffer([]);
  this.offsetsResponder = function(){};
  this.fetchResponder = function(){};
};


Consumer.prototype.onOffsets = function(handler){  this.offsetsResponder = handler; };
Consumer.prototype.onFetch = function(handler){  this.fetchResponder = handler; };

Consumer.prototype._setRequestMode = function(mode){
  this.requestMode = mode;
};

Consumer.prototype._unsetRequestMode = function(){
  this.requestMode = null;
};

Consumer.prototype.connect = function(cb){
  var that = this;
  if (this.port <= 0 || this.port >= 65536) {
    return cb(new Error('Port should be > 0 and < 65536'));
  }
  this.socket = net.createConnection({port : this.port, host : this.host}, function(){
    cb();
  });
  this.responseBuffers = [];
  this.responseLength = 0;
  this.responseBuffersLength = 0;

  this.socket.on('end', function(){
  });
  this.socket.on('timeout', function(){
  });
  this.socket.on('drain', function(){
  });
  this.socket.on('error', function(err){
    cb(err);
  });
  this.socket.on('close', function(){
  });
  this.socket.on('data', function(data){
    var responseHeaderSize = 8;
    if (that.maxMessageSize + responseHeaderSize < (that.responseBuffer.length + data.length)){
      var handler = that.requestMode === 'fetch' ? that.fetchResponder : that.offsetsResponder;
      var errorMessage = 'Max message size is ' + that.maxMessageSize + ' bytes, was exceeded by buffer of size ' +
        (that.responseBuffer.length + data.length) + ' Possible causes: bad offset (current offset: ' + that.offset +
        '), corrupt log, message larger than max message size.';
      return handler(new Error(errorMessage));
    }
    that.responseBuffers.push(data);
    that.responseBuffersLength += data.length;
    if (that.responseBuffersLength > 4) {
      var tmpBuffer = null;
      if (!that.responseLength) {
        tmpBuffer = Buffer.concat(that.responseBuffers)
        that.responseLength = tmpBuffer.readUInt32BE(0) + 4; // the encoded length does not include the 4 byte encoded length
      }

      if (that.responseBuffersLength >= that.responseLength) {
        that.responseBuffer = !!tmpBuffer ? tmpBuffer: Buffer.concat(that.responseBuffers);
        that.responseBuffers = [];
        that.responseLength = 0;
        that.responseBuffersLength = 0;
        switch(that.requestMode){
          case "fetch":
            that.handleFetchData(that.fetchResponder);
            break;
          case "offsets":
            that.handleOffsetsData(that.offsetsResponder);
            break;
          default:
            throw "Got a response when no response was expected!";
        }
      }
    }
  });
};

Consumer.prototype.handleFetchData = function(cb){
  // message fetch
  var err = null;
  var response = null;
  try {
    response = FetchResponse.fromBytes(this.responseBuffer);
    this.responseBuffer = new Buffer([]);
  } catch(ex){
    if (ex === "incomplete response"){
      // don't bother parsing.  quit out of this handler.  wait for more data.
      return;
    } else {
      return cb(ex);
    }
  }
  if (response.error){
    return cb(errorCodeToError(response.error));
  }
  var messages = response.messages;
  var messageLength = 0;
  for (var i = 0; i < messages.length; i++) {
    messageLength += messages[i].payload.length + this.MESSAGE_HEADER_LENGTH;
  }
  this.incrementOffset(messageLength);
  this._unsetRequestMode();
  return cb(null, response);
};

Consumer.prototype.handleOffsetsData = function(cb){
  var response;
  try {
    response = OffsetsResponse.fromBytes(this.responseBuffer);
  } catch(ex){
    if (ex === "incomplete response"){
      // don't bother parsing.  quit out of this handler.  wait for more data.
      return;
    } else {
      return cb(ex);
    }
  }
  if (response.error) {
    return cb(errorCodeToError(response.error));
  }
  this._unsetRequestMode();
  this.responseBuffer = this.responseBuffer.slice(response.byteLength());
  //this.responseBuffer = new Buffer([]);
  var offsets = response.offsets;
  return cb(null, offsets);
};

Consumer.prototype.sendConsumeRequest = function(cb){
  if (this.offset === null || this.offset === undefined || !this.offset.eq) {
    return cb("offset was " + this.offset);
  }
  this._setRequestMode("fetch");
  this.onFetch(cb);
  var that = this;
  var fetchRequest =  new FetchRequest(this.offset, this.topic, this.partition, this.maxMessageSize);
  var request = fetchRequest.toBytes();
  this.socket.write( request );
};

Consumer.prototype.consume = function(cb){
  var consumer = this;
  var offset;
  if (this.offset === null){
      this.getOffsets(function(err, offsets){
        if (err){
          return cb(err);
        }
        offset = offsets[0];
        consumer.setOffset(offset);
        consumer.sendConsumeRequest(function(err, fetchResponse){
          handleFetchResponse(consumer, err, fetchResponse, cb);
        });
      });
  } else {
    consumer.sendConsumeRequest(function(err, fetchResponse){
      handleFetchResponse(consumer, err, fetchResponse, cb);
    });
  }

};

Consumer.prototype.setOffset = function(offset){
  this.offset = bignum(offset);
};
Consumer.prototype.incrementOffset = function(value){
  this.setOffset(this.offset.add(bignum(value)));
};

var handleFetchResponse = function(consumer, err, response, cb){
  if (!!err) return cb(err);
  cb(null, response.messages);
};

Consumer.prototype.getOffsets = function(cb){
  this.onOffsets(cb);
  this._setRequestMode("offsets");
  var that = this;
  var request = new OffsetsRequest(this.topic, this.partition, -1, this.MAX_OFFSETS);
  this.socket.write(request.toBytes());

};

Consumer.prototype.getLatestOffset = function(cb){
  this.onOffsets(function(err, offsets){
    cb(err, offsets[0]);
  });
  this._setRequestMode("offsets");
  var that = this;
  var request = new OffsetsRequest(this.topic, this.partition, -1, 1);
  this.socket.write(request.toBytes());
};

Consumer.prototype.getEarliestOffset = function(cb){
  this.onOffsets(function(err, offsets){
    cb(err, offsets[0]);
  });
  this._setRequestMode("offsets");
  var that = this;
  var request = new OffsetsRequest(this.topic, this.partition, -2, 1);
  this.socket.write(request.toBytes());
};


var errorCodeToError = function(code){
  switch(code){
    case -1 : return new Error("Unknown");
    case 1 : return new Error("OffsetOutOfRange");
    case 2  : return new Error("InvalidMessage");
    case 3  : return new Error("WrongPartition");
    case 4  : return new Error("InvalidFetchSize");
    default : return new Error("Unknown error code: " + code);
  }
};

module.exports = Consumer;

