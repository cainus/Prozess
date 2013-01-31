var net = require('net');
var _ = require('underscore');
var Message = require('./Message');
var BufferMaker = require('buffermaker');
var bignum = require('bignum');
var OffsetsResponse = require('./OffsetsResponse');
var FetchRequest = require('./FetchRequest');
var OffsetsRequest = require('./OffsetsRequest');
var FetchResponse = require('./FetchResponse');

var Consumer = function(options){
  this.MAX_MESSAGE_SIZE = 1024 * 1024; // 1 megabyte
  this.DEFAULT_POLLING_INTERVAL = 2; // 2 seconds
  this.MAX_OFFSETS = 1;
  this.LATEST_OFFSET = -1;
  this.EARLIEST_OFFSET = -2;

  options = options || {};
  this.topic = options.topic || 'test';
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;
  this.offset = options.offset ? bignum(options.offset) : null;
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
  this.socket = net.createConnection({port : this.port, host : this.host}, function(){
    cb();
  });

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
    that.responseBuffer = Buffer.concat([that.responseBuffer, data]); 
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
  });
};

Consumer.prototype.handleFetchData = function(cb){
  // message fetch
  var err = null;
  var response = null;
  try {
    response = FetchResponse.fromBytes(this.responseBuffer);
    this.responseBuffer = this.responseBuffer.slice(response.toBytes().length);
  } catch(ex){
    if (ex === "incomplete response"){
      // don't bother parsing.  quit out of this handler.  wait for more data.
      return;
    } else {
      return cb(ex);
    }
  }
  var messages = response.messages;
  var messageLength = 0;
  _.each(messages, function(message){
    messageLength += message.toBytes().length;
  });
  this.incrementOffset(messageLength);
  this._unsetRequestMode();
  return cb(null, response);
  
};

Consumer.prototype.handleOffsetsData = function(cb){
  var res;
  try {
    res = OffsetsResponse.fromBytes(this.responseBuffer);
  } catch(ex){
    if (ex === "incomplete response"){
      // don't bother parsing.  quit out of this handler.  wait for more data.
      return;
    } else {
      return cb(ex);
    }
  }
  if (!!res.error) {
    return cb(res.error);
  }
  this._unsetRequestMode();
  this.responseBuffer = this.responseBuffer.slice(res.byteLength());
  var offsets = res.offsets;
  return cb(null, res);
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
      this.getOffsets(function(err, offsetsResponse){
        if (err){
          return cb(err);
        }
        var offsets = offsetsResponse.offsets;
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
  if (!!err){
  }
  if (err){
    return cb(err);
  }
  cb(null, response.messages);
};

Consumer.prototype.getOffsets = function(cb){
  this.onOffsets(cb);
  this._setRequestMode("offsets");
  var that = this;
  var request = new OffsetsRequest(this.topic, this.partition, -1, this.MAX_OFFSETS);
  this.socket.write(request.toBytes());
 
};


module.exports = Consumer;

