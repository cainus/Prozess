var net = require('net');
var _ = require('underscore');
var Message = require('./message').Message;
var BufferMaker = require('buffermaker');
var binary = require('binary');

var Consumer = function(options){
  this.MAX_SIZE = 1024 * 1024; // 1 megabyte
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

  this.topic = options.topic || 'test';
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;
  this.offset = options.offset;
  this.max_size = options.max_size || this.MAX_SIZE;
  this.polling = options.polling  || this.DEFAULT_POLLING_INTERVAL;
  this.request_type = null;
  this.unprocessedData = new Buffer([]);
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

Consumer.prototype._handleData = function(data){
    var messageSet = Message.parseFrom(data);
    console.log("message: ", messageSet);

    // remove that data from unprocessedData
    //console.log("removing message from ", that.unprocessedData);
    //that.unprocessedData = that.unprocessedData.slice((expectedLength - data.length) + 4);
    //console.log("unprocessed is now  ", that.unprocessedData);
    
    //that.onMessage(message);

    that.offset += messageSet.size;  // increment the offset, because we got new data

};


Consumer.prototype.connect = function(){
  var that = this;
  this.socket = net.createConnection(this.port, this.host);
  this.socket.on('connect', function(){
    console.log("connected");
  });

  this.socket.on('data', function(data){
    
    that._handleData(data);

  });



  this.socket.on('end', function(){
    console.log("end");
  });
  this.socket.on('timeout', function(){
    console.log("timeout");
  });
  this.socket.on('drain', function(){
    console.log("connected");
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





Consumer.prototype.encodeRequest =
    function( request_type, topic, partition, offset, max_size){

      return new BufferMaker()
              .UInt16BE(request_type)
              .UInt16BE(topic.length)
              .string(topic)
              .UInt32BE(partition)
              .Int64BE(offset)
              .UInt32BE(max_size)
              .make();
      //offset       = [offset].pack("q").reverse # DIY 64bit big endian integer
};

Consumer.prototype.sendConsumeRequest = function(){
  this.socket.write( this.encodedRequestSize() );
  this.socket.write( this.encodeRequest( this.RequestType.FETCH,
                                         this.topic,
                                         this.partition,
                                         this.offset,
                                         this.max_size));
};

exports.Consumer = Consumer;



