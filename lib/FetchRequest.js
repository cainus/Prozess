var BufferMaker = require('buffermaker');
var Request = require('./Request');

var FetchRequest = function(offset, topic, partition, maxMessageSize) {
  this.offset = offset;
  this.topic = topic;
  this.partition = partition;
  this.maxMessageSize = maxMessageSize;
  this.RequestTypes = {
    FETCH : 1 
  };
};

FetchRequest.prototype.toBytes = function(){
  var body = new BufferMaker()
  .Int64BE(this.offset)
  .UInt32BE(this.maxMessageSize)
  .make();
  var req = new Request(this.topic, this.partition, Request.Types.FETCH, body);
  return req.toBytes();
};

module.exports = FetchRequest;
