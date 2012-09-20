var binary = require('binary');
var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var Protocol = require('../lib/Protocol');
var _ = require('underscore');


var ProduceRequest = function(topic, partition, messages){
  this.topic = topic;
  this.partition = partition;
  this.messages = messages;
  this.requestType = 0; // produce
};

/*

 0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                         REQUEST HEADER                        /
  /                                                               /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         MESSAGES_LENGTH                       |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                                                               /
  /                            MESSAGES                           /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  MESSAGES_LENGTH = int32 // Length in bytes of the MESSAGES section
  MESSAGES = Collection of MESSAGES (see above)

*/
ProduceRequest.prototype.toBytes = function(){
  var messages = this.messages;
  var that = this;
  var messageBuffers = [];
  _.each(messages, function(message){
    console.log("producing message: ", message);
    var bytes = message.toBytes();
    console.log("bytes: ", bytes);
    messageBuffers.push(bytes);
  });
  var messagesBuffer = Buffer.concat(messageBuffers);
  console.log("encoding produce: ", this.topic, this.partition);
  var header = encodeRequestHeader(this.requestType, this.topic, this.partition, messagesBuffer.length + 4);
                                                                                        // 4 bytes for messages size
  var messagesLength = new BufferMaker().UInt32BE(messagesBuffer.length).make();
  var encodedRequest = Buffer.concat([header, messagesLength, messagesBuffer]);
  console.log("returning: ", encodedRequest);
  return encodedRequest;
};


module.exports = ProduceRequest;

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
