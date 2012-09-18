var net = require('net');
var _ = require('underscore');
var Message = require('./message').Message;
var BufferMaker = require('buffermaker');
var OffsetsResponse = require('./offsetsResponse');
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

};

/*
Consumer.prototype.addData = function(data){
  console.log("want to add: ", data);
  var that = this;
  console.log("unprocessData", that.unprocessedData);
  that.unprocessedData = Buffer.concat([that.unprocessedData, data]);
                                  // add data to unprocessed buffer
  console.log("total unprocessedData: ", that.unprocessedData);
  that.offset += data.length;  // increment the offset, because we got new data
  console.log("offset is now: ", that.offset);
};*/



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
};


Consumer.prototype.onMessage = function(cb){
  cb(err, message);
};

Consumer.prototype.sendConsumeRequest = function(offset, cb){
  this.requestMode = "messages";
  var that = this;
  this.socket.on('data', function(data){
    if (that.requestMode === "messages"){
      console.log("consumer got data: ", data);
      // message fetch
      var err = null;
      var messageSet = null;
      try {
        messageSet = Message.parseFrom(data);
        console.log("message: ", messageSet);
        that.requestMode = null;
      } catch(ex){
        console.log("ex: ", ex);
        that.requestMode = null;
        err = ex;
      }
      cb(err, messageSet);
    }
  });
  this.socket.write( this.protocol.encodeFetchRequest( offset ));
  console.log("wrote to the socket");
};

Consumer.prototype.consume = function(cb){
  console.log("in consume");
  var consumer = this;
   if (this.offset === undefined){
       this.getOffsets(function(err, offsets){
         console.log("got offsets: ", offsets);
         var offset = offsets[0];
         console.log("setting consumer offset");
         consumer.offset = offset;
         consumer.sendConsumeRequest(offset, function(err, messageSet){
           console.log("sending consume request ", err, messageSet);
           cb(err, messageSet.messages);
         });
       });
   } else {
     console.log("offset: ", this.offset);
     throw("TODO: handle when offset is defined");
   }

};

Consumer.prototype.getOffsets = function(cb){
  this.requestMode = "offsets";
  var that = this;
  this.socket.on('data', function(data){
    if (that.requestMode === "offsets"){
      that.responseBuffer = Buffer.concat([that.responseBuffer, data]); 
      OffsetsResponse.parseFrom(that.responseBuffer, function(err, res){
        if (!!err) {
          if (err === "incomplete response"){
            // do nothing.  we'll wait for more data.
            return;
          } else {
            return cb(err);
          }
        }
        that.requestMode = null;
        that.responseBuffer = new Buffer([]);
        var offsets = res.offsets;
        cb(null, offsets);
      });
    }
  });
  this.socket.write(this.protocol.encodeOffsetsRequest(-1, this.MAX_OFFSETS));
 
};


module.exports = Consumer;

