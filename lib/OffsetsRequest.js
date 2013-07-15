var bignum = require('bignum');
var binary = require('binary');
var Request = require('./Request');
var BufferMaker = require('buffermaker');
var _ = require('underscore');




var OffsetsRequest = function(topic, partition, since, maxOffsets){
  this.topic = topic;
  this.partition = partition;
  this.since = since;
  this.maxOffsets = maxOffsets;
};

OffsetsRequest.prototype.toBytes = function(){
  var maxOffsets = this.maxOffsets;
  var time;
  switch(this.since){
    case -1:
      time = new Buffer([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
      break;
    case -2:
      time = new Buffer([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE]);
      break;
    default :
      throw "TODO: support unix timestamp";
  }
  var requestBody = new BufferMaker()
                        .string(time)
                        .UInt32BE(maxOffsets)
                        .make();
                        // 12 for the body
  var req = new Request(this.topic, this.partition, Request.Types.OFFSETS, requestBody);
  return req.toBytes();
};


module.exports = OffsetsRequest;
