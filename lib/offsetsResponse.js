var bignum = require('bignum');
var binary = require('binary');

var OffsetsResponse = function(offsets){
  this.offsets = offsets; 
};

OffsetsResponse.parseFrom = function(response){
  var unpacked = binary.parse(response)
  .word32bu('length')
  .word16bu('error')
  .word32bu('offsetsCount')
  .vars;

  var offsets = [];
  var i = unpacked.offsetsCount;
  var processed = 10;

  while(i > 0){
    var offset = response.slice(processed, processed + 8);
    offsets.push(bignum.fromBuffer(offset));
    processed += 8;
    i -= 1;
  }

   return new OffsetsResponse(offsets);


};

module.exports = OffsetsResponse;
