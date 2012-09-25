var bignum = require('bignum');
var binary = require('binary');
var Response = require('./Response');
var BufferMaker = require('buffermaker');
var _ = require('underscore');

/*
 https://cwiki.apache.org/KAFKA/writing-a-driver-for-kafka.html

   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                         RESPONSE HEADER                       /
  /                                                               /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                         NUMBER_OFFSETS                        |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  /                       OFFSETS (0 or more)                     /
  /                                                               /
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  NUMBER_OFFSETS = int32 // How many offsets are being returned
  OFFSETS = int64[] // List of offsets
*/
var OffsetsResponse = function(error, offsets, length){
  this.error = error;
  this.offsets = offsets;
  this.length = length;
};


OffsetsResponse.OFFSET_BYTE_SIZE = 8;

OffsetsResponse.fromBytes = function(bytes){
  console.log("fromBytes: ", bytes);
  var response = Response.fromBytes(bytes);
  var error = response.error;
  if (error !== Response.Errors.NoError){
    return new OffsetsResponse(error, [], body.length + 4);
  }
  var body = response.body;
  var vars = binary.parse(body)
                   .word32bu('offsetsCount')
                   .buffer('rest', (body.length - 4))
                   .vars;
  var offsetsCount = vars.offsetsCount;
  var offsets = [];
  var rest = vars.rest;

  while( (rest.length >= OffsetsResponse.OFFSET_BYTE_SIZE) && (offsets.length < offsetsCount) ){
    var offset = rest.slice(0, OffsetsResponse.OFFSET_BYTE_SIZE);
    rest = rest.slice(OffsetsResponse.OFFSET_BYTE_SIZE);
    offsets.push(bignum.fromBuffer(offset));
  }

  return new OffsetsResponse(Response.Errors.NoError, offsets, body.length + 4);  // 4 bytes for size field

};


OffsetsResponse.prototype.toBytes = function(){
  var offsetsBuffer = new BufferMaker();
  _.each(this.offsets, function(offset){
    offsetsBuffer = offsetsBuffer.Int64BE(offset);
  });
  offsetsBuffer = offsetsBuffer.make();
  var body = new BufferMaker()
   .UInt32BE(this.offsets.length)    // number of offsets 
   .string(offsetsBuffer)   // offset 1
   .make();

  return new Response(this.error, body).toBytes();
};

module.exports = OffsetsResponse;
