var net = require('net');
var _ = require('underscore');
var Message = require('./message').Message;
var BufferMaker = require('buffermaker');
var OffsetsResponse = require('./offsetsResponse');
var RequestTypes = require('./RequestTypes');
var Client = require('./client');
var binary = require('binary');

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
  this.request_type = null;
  this.unprocessedData = new Buffer([]);

  this.requestMode = null;
  this.responseBuffer = new Buffer([]);

};

Consumer.prototype.addData = function(data){
  console.log("want to add: ", data);
  var that = this;
  console.log("unprocessData", that.unprocessedData);
  that.unprocessedData = Buffer.concat([that.unprocessedData, data]);
                                  // add data to unprocessed buffer
  console.log("total unprocessedData: ", that.unprocessedData);
  that.offset += data.length;  // increment the offset, because we got new data
  console.log("offset is now: ", that.offset);
};



Consumer.prototype.connect = function(cb){
  var that = this;
  this.socket = net.createConnection(this.port, this.host);
  this.socket.on('connect', function(){
    cb();
    console.log("connected");
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

Consumer.prototype.encodedRequestSize = function(){
  var size = 2 + 2 + this.topic.length + 4 + 8 + 4;
  //[size].pack("N")
  console.log("size: ", size);
  var buffer = new Buffer(4);
  buffer.writeUInt32BE(size, 0);
  return buffer;

};


Consumer.prototype.onMessage = function(message){
  console.dir(message);
};


Consumer.prototype.encodeOffsetsRequest = function(time){
  if (time === -1){
    time = new Buffer([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
  }
  var requestBody = new BufferMaker()
                        .string(time)
                        .UInt32BE(this.MAX_OFFSETS)
                        .make();
                        // 12 for the body
  var requestBodyLength = requestBody.length;
  console.log('12?', requestBodyLength);
  var requestType = RequestTypes.OFFSETS;
  var topic = this.topic;
  var partition = this.partition;
  console.log('encodeRequestHeader');
  console.log(requestType, topic, partition, requestBodyLength);
  var header = Client.encodeRequestHeader(requestType, topic, partition, requestBodyLength);
  console.log('headerlength: ', header.length);
  return Buffer.concat([header, requestBody]);
};


Consumer.prototype.encodeRequest =
    function( request_type, topic, partition, offset, maxMessageSize){

      return new BufferMaker()
              .UInt16BE(request_type)
              .UInt16BE(topic.length)
              .string(topic)
              .UInt32BE(partition)
              .Int64BE(offset)
              .UInt32BE(maxMessageSize)
              .make();
      //offset       = [offset].pack("q").reverse # DIY 64bit big endian integer
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
  this.socket.write( this.encodedRequestSize() );
  this.socket.write( this.encodeRequest( this.RequestType.FETCH,
                                         this.topic,
                                         this.partition,
                                         offset,
                                         this.maxMessageSize));
                                         console.log("wrote to the socket");
};

Consumer.prototype.consume = function(cb){
  var consumer = this;
   if (this.offset === undefined){
       this.getOffsets(function(err, offsets){
         console.log("got offsets: ", offsets);
         var offset = offsets[0]; 
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
  this.socket.write(this.encodeOffsetsRequest(-1));
 
};


module.exports = Consumer;


