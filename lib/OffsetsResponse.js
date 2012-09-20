var bignum = require('bignum');
var binary = require('binary');


var OffsetsResponse = function(offsets, size){
  this.offsets = offsets; 
  this.size = size;
};

OffsetsResponse.fromBytes = function(response, cb){
  var unpacked; 
  binary.parse(response)
  .word32bu('length')
  .tap(function(vars){
    if (response.length < vars.length){
      cb("incomplete response");
    } else {
      this.word16bu('error');
      this.word32bu('offsetsCount');
      unpacked = this.vars;
      var offsets = [];
      var i = unpacked.offsetsCount;
      var processed = 10;

      while(i > 0){
        var offset = response.slice(processed, processed + 8);
        offsets.push(bignum.fromBuffer(offset));
        processed += 8;
        i -= 1;
      }

      cb(null, new OffsetsResponse(offsets, vars.length + 4));  // 4 bytes for size field
    }
  });



};

module.exports = OffsetsResponse;
