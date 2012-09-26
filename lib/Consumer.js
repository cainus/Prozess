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
  this.offset = options.offset || null;
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
  console.log("REQUEST MODE: ", mode);
};

Consumer.prototype._unsetRequestMode = function(){
  this.requestMode = null;
  console.log("REQUEST MODE: [NONE]");
};

Consumer.prototype.connect = function(cb){
  console.log("in connect!");
  var that = this;
  this.socket = net.createConnection({port : this.port, host : this.host}, function(){
    console.log("connected");
    cb();
  });

  this.socket.on('end', function(){
    console.log("end");
  });
  this.socket.on('timeout', function(){
    console.log("timeout");
  });
  this.socket.on('drain', function(){
    console.log("drain");
  });
  this.socket.on('error', function(err){
    console.log("error: ", err);
  });
  this.socket.on('close', function(){
    console.log("close");
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
        console.log("wth: ", data);
        throw "Got a response when no response was expected!";

    }
  });
};

Consumer.prototype.handleFetchData = function(cb){
  console.log("FETCH MODE");
  // message fetch
  var err = null;
  var response = null;
  try {
    console.log("REALLY WANT TO GET TO FETCH RESPONSE...");
    response = FetchResponse.fromBytes(this.responseBuffer);
    console.log('===============================================');
    console.log("FETCH RESPONSE: ", response);
    this.responseBuffer = this.responseBuffer.slice(response.toBytes().length);
  } catch(ex){
    if (ex === "incomplete response"){
      // don't bother parsing.  quit out of this handler.  wait for more data.
      return;
    } else {
      console.log("error from throw");
      console.log(ex.stack);
      return cb(ex);
    }
  }
  var messages = response.messages;
  var messageLength = 0;
  _.each(messages, function(message){
    messageLength += message.toBytes().length;
  });
  console.log("BYTELENGTH: ", messageLength);
  this.incrementOffset(messageLength);
  this._unsetRequestMode();
  return cb(null, response);
  
};

Consumer.prototype.handleOffsetsData = function(cb){
  console.log("got data in offsets mode");
  console.log("responseBuffer: ", this.responseBuffer);
  console.log("concatted responseBuffer: ", this.responseBuffer);
  var res;
  try {
    res = OffsetsResponse.fromBytes(this.responseBuffer);
  } catch(ex){
    if (ex === "incomplete response"){
      // don't bother parsing.  quit out of this handler.  wait for more data.
      return;
    } else {
      console.log("error from throw");
      return cb(ex);
    }
  }
  if (!!res.error) {
    console.log("error from return val");
    return cb(res.error);
  }
  this._unsetRequestMode();
  console.log("subtracting first ", res.byteLength(), " bytes.");
  this.responseBuffer = this.responseBuffer.slice(res.byteLength());
  var offsets = res.offsets;
  console.log("REzzzzzzzzzz", res);
  console.log("============================== OFFSETS!!! ", offsets);
  return cb(null, res);
};

Consumer.prototype.sendConsumeRequest = function(cb){  
  console.log("sendConsume called with this.offset: ", this.offset);
  if (this.offset === null || this.offset === undefined || !this.offset.eq) {
    return cb("offset was ", this.offset);
  }
  this._setRequestMode("fetch");
  this.onFetch(cb);
  var that = this;
  var fetchRequest =  new FetchRequest(this.offset, this.topic, this.partition, this.maxMessageSize);
  var request = fetchRequest.toBytes();
  this.socket.write( request );
  console.log("wrote a fetch request to the socket", request);
};

Consumer.prototype.consume = function(cb){
  console.log("in consume");
  var consumer = this;
  var offset;
  if (this.offset === null){
      this.getOffsets(function(err, offsetsResponse){
        if (err){
          console.log(err);
          console.log(err.stack);
          return cb(err);
        }
        console.log("got offsets response: ", offsetsResponse);
        var offsets = offsetsResponse.offsets;
        offset = offsets[0];
        console.log("setting consumer offset to ", offset);
        consumer.setOffset(offset);
        console.log("set consumer offset to ", this.offset);
        consumer.sendConsumeRequest(function(err, fetchResponse){
          handleFetchResponse(consumer, err, fetchResponse, cb);
        });
      });
  } else {
    console.log("offset: ", offset);
    consumer.sendConsumeRequest(function(err, fetchResponse){
      handleFetchResponse(consumer, err, fetchResponse, cb);
    });
  }

};

Consumer.prototype.setOffset = function(offset){
  this.offset = bignum(offset);
};
Consumer.prototype.incrementOffset = function(value){
  console.log("adding ", value, " to ", this.offset);
  this.setOffset(this.offset.add(bignum(value)));
  console.log("got : ", this.offset);
};


var handleFetchResponse = function(consumer, err, response, cb){
  console.log("---> back with the response to consume-request (undefined)");
  if (!!err){
    console.log("err from sendConsumerRequest: ", err);
  }
  console.log("---> sendConsumeRequest CB input", err, response);
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
  console.log("writing offsets request: ", request);
  this.socket.write(request.toBytes());
  console.log("request written");
 
};


module.exports = Consumer;

