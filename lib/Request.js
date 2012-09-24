var BufferMaker = require('buffermaker');

var Request = function(topic, partition, type, body){
  if (type < 0 || type > 4){ 
    throw "Invalid request type: " + type;
  }
  if (!Buffer.isBuffer(body)){
    throw "The body parameter must be a Buffer object.";
  }
  this.topic = topic;
  this.partition = partition;
  this.type = type;
  this.body = body;
};


Request.Types = {
    PRODUCE      : 0,
    FETCH        : 1,
    MULTIFETCH   : 2,
    MULTIPRODUCE : 3,
    OFFSETS      : 4
};

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
Request.prototype.toBytes = function(){
  return new BufferMaker()
        .UInt32BE(2 + 2 + this.topic.length + 4 + this.body.length) 
        .UInt16BE(this.type)
        .UInt16BE(this.topic.length)
        .string(this.topic)
        .UInt32BE(this.partition)
        .string(this.body)
        .make();
};

module.exports = Request;
