var BufferMaker = require('buffermaker');

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
  var header = encodeRequestHeader(this.RequestTypes.FETCH, this.topic, this.partition, body.length);
  return Buffer.concat([header, body]);
};

var encodeRequestHeader = function(requestType, topic, partition, requestBodyLength){
  var requestHeader = new BufferMaker()
  /* request length = request type + topic length's length + topic length + partition + body length */
  .UInt32BE(2 + 2 + topic.length + 4 + requestBodyLength) 
  .UInt16BE(requestType)
  .UInt16BE(topic.length)
  .string(topic)
  .UInt32BE(partition)
  .make();
  return requestHeader;

};
module.exports = FetchRequest;
