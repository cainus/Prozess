var BufferMaker = require('buffermaker');
var binary = require('binary');
var assert = require('assert');
var _ = require('underscore');

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
  var topic = new Buffer(this.topic);
  var body = new Buffer(this.body);
  assert(Buffer.isBuffer(body));
  assert(Buffer.isBuffer(topic));
  return new BufferMaker()
        .UInt32BE(2 + 2 + topic.length + 4 + body.length) 
        .UInt16BE(this.type)
        .UInt16BE(topic.length)
        .string(topic)
        .UInt32BE(this.partition)
        .string(body)
        .make();
};

Request.fromBytes = function(bytes) {
  var vars = binary.parse(bytes)
                     .word32bu('length')
                     .word16bu('type')
                     .word16bu('topicLength')
                     .tap( function(vars) {
                       this.buffer('topic', vars.topicLength);
                     })
                     .word32bu('partition')
                     .tap( function(vars) {
                       this.buffer('body', vars.length - 2 - 2 - 4 - vars.topicLength);
                     })
                     .vars;
  return new Request(vars.topic.toString(), vars.partition, vars.type, vars.body);
};

module.exports = Request;
