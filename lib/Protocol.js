var BufferMaker = require('buffermaker');
var Message = require('./Message');
var _ = require('underscore');


/*  This class abstracts away all byte-handling, so there should be no byte handling 
 *  elsewhere in the lib other than here and in any message-specific classes that this
 *  class uses.
 */

var Protocol = function(topic, partition, maxMessageSize){
  this.topic = topic;
  this.partition = partition;
  this.maxMessageSize = maxMessageSize;
  this.RequestTypes = {
    PRODUCE      : 0,
    FETCH        : 1,
    MULTIFETCH   : 2,
    MULTIPRODUCE : 3,
    OFFSETS      : 4
  };
};



Protocol.prototype.encodeOffsetsRequest = function(time, maxOffsets){
  if (time === -1){
    time = new Buffer([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
  } else {
    throw "TODO: support unix timestamp and -2";
  }
  var requestBody = new BufferMaker()
                        .string(time)
                        .UInt32BE(maxOffsets)
                        .make();
                        // 12 for the body
  var requestBodyLength = requestBody.length;
  var requestType = this.RequestTypes.OFFSETS;
  var topic = this.topic;
  var partition = this.partition;
  var header = encodeRequestHeader(requestType, topic, partition, requestBodyLength);
  return Buffer.concat([header, requestBody]);
};





module.exports = Protocol;


/*
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                       REQUEST_LENGTH                          |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |         REQUEST_TYPE          |        TOPIC_LENGTH           |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                                                               /
  /                    TOPIC (variable length)                    /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                           PARTITION                           |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  REQUEST_LENGTH = int32 // Length in bytes of entire request (excluding this field)
  REQUEST_TYPE   = int16 // See table below
  TOPIC_LENGTH   = int16 // Length in bytes of the topic name

  TOPIC = String // Topic name, ASCII, not null terminated
                 // This becomes the name of a directory on the broker, so no
                 // chars that would be illegal on the filesystem.

  PARTITION = int32 // Partition to act on. Number of available partitions is
                    // controlled by broker config. Partition numbering
                    // starts at 0.

  ============  =====  =======================================================
  REQUEST_TYPE  VALUE  DEFINITION
  ============  =====  =======================================================
  PRODUCE         0    Send a group of messages to a topic and partition.
  FETCH           1    Fetch a group of messages from a topic and partition.
  MULTIFETCH      2    Multiple FETCH requests, chained together
  MULTIPRODUCE    3    Multiple PRODUCE requests, chained together
  OFFSETS         4    Find offsets before a certain time (this can be a bit
                       misleading, please read the details of this request).
  ============  =====  =======================================================
*/
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
