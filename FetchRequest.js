var BufferMaker = require('buffermaker');
var Request = require('./Request');

var FetchRequest = function(offset, topic, partition, maxMessageSize) {
  this.offset = offset;
  this.topic = topic;
  this.partition = partition;
  this.maxMessageSize = maxMessageSize;
};

 /*
    0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                         REQUEST HEADER                        /
  /                                                               /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                             OFFSET                            |
  |                                                               |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                            MAX_SIZE                           |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  REQUEST_HEADER = See REQUEST_HEADER above
  OFFSET   = int64 // Offset in topic and partition to start from
  MAX_SIZE = int32 // MAX_SIZE of the message set to return
  */
FetchRequest.prototype.toBytes = function(){
  var body = new BufferMaker()
  .Int64BE(this.offset)
  .UInt32BE(this.maxMessageSize)
  .make();
  var req = new Request(this.topic, this.partition, Request.Types.FETCH, body);
  return req.toBytes();
};

module.exports = FetchRequest;
