var net = require('net');
var _ = require('underscore');
var Message = require('./Message');
var BufferMaker = require('buffermaker');
var OffsetsResponse = require('./OffsetsResponse');
var Protocol = require('./Protocol');

var Consumer = function(options){
  this.MAX_MESSAGE_SIZE = 1024 * 1024; // 1 megabyte
  this.DEFAULT_POLLING_INTERVAL = 2; // 2 seconds
  this.MAX_OFFSETS = 1;
  this.LATEST_OFFSET = -1;
  this.EARLIEST_OFFSET = -2;
  this.RequestType = {
    PRODUCE      : 0,
    FETCH        : 1,
    MULTIFETCH   : 2,
    MULTIPRODUCE : 3,
    OFFSETS      : 4
  };

  options = options || {};
  this.topic = options.topic || 'test';
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;
  this.offset = options.offset;
  this.maxMessageSize = options.maxMessageSize || this.MAX_MESSAGE_SIZE;
  this.polling = options.polling  || this.DEFAULT_POLLING_INTERVAL;
  this.unprocessedData = new Buffer([]);

  this.requestMode = null;
  this.responseBuffer = new Buffer([]);
  this.protocol = new Protocol(this.topic, this.partition, this.MAX_MESSAGE_SIZE);
  this.offsetsResponder = function(){};
  this.fetchResponder = function(){};

};


Consumer.prototype.onOffsets = function(handler){  this.offsetsResponder = handler; };
Consumer.prototype.onFetch = function(handler){  this.fetchResponder = handler; };

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
    switch(that.requestMode){
      case "fetch":
        console.log("consumer got data: ", data);
        // message fetch
        var err = null;
        var messageSet = null;
        try {
          console.log("parsing from: ", data);
          messageSet = Message.parseFrom(data);
          console.log("messageSet: ", messageSet);
          that.requestMode = null;
          that.responseBuffer = new Buffer([]);
          that.fetchResponder(null, messageSet);
        } catch(ex){
          console.log("ex: ", ex);
          that.requestMode = null;
          err = ex;
          that.fetchResponder(err);
        }
      break;



      case "offsets":
        console.log("got data in offsets mode");
        console.log("responseBuffer: ", that.responseBuffer);
        console.log("data: ", data);
        that.responseBuffer = Buffer.concat([that.responseBuffer, data]); 
        console.log("concatted responseBuffer: ", that.responseBuffer);
        OffsetsResponse.parseFrom(that.responseBuffer, function(err, res){
          if (!!err) {
            if (err === "incomplete response"){
              // do nothing.  we'll wait for more data.
              console.log("incomplete. do nothing.");
              return;
            } else {
              return that.offsetsResponder(err);
            }
          }
          that.requestMode = null;
          that.responseBuffer = new Buffer([]);
          var offsets = res.offsets;
          console.log("============================== OFFSETS!!! ", offsets);
          return that.offsetsResponder(null, offsets);
        });
      break;


      default:
        throw "Got a response when no response was expected!";

    }
  });
};


Consumer.prototype.sendConsumeRequest = function(offset, cb){
  this.requestMode = "fetch";
  this.onFetch(cb);
  var that = this;
  this.socket.write( this.protocol.encodeFetchRequest( offset ));
  console.log("wrote to the socket");
};

Consumer.prototype.consume = function(cb){
  console.log("in consume");
  var consumer = this;
  var offset;
  if (this.offset === undefined){
      this.getOffsets(function(err, offsets){
        console.log("got offsets: ", offsets);
        offset = offsets[0];
        console.log("setting consumer offset");
        consumer.offset = offset;
        consumer.sendConsumeRequest(offset, function(err, messageSet){
          if (err){
            return cb(err);
          }
          //console.log("sending consume request ", err, messageSet);
          cb(err, messageSet.messages);
        });
      });
  } else {
    offset = this.offset;
    console.log("offset: ", offset);
    consumer.sendConsumeRequest(offset, function(err, messageSet){
      if (err){
        return cb(err);
      }
      //console.log("sending consume request ", err, messageSet);
      cb(err, messageSet.messages);
    });
  }

};

Consumer.prototype.getOffsets = function(cb){
  this.onOffsets(cb);
  this.requestMode = "offsets";
  var that = this;
  var request = this.protocol.encodeOffsetsRequest(-1, this.MAX_OFFSETS);
  console.log("writing offsets request: ", request);
  this.socket.write(request);
  console.log("request written");
 
};


module.exports = Consumer;

