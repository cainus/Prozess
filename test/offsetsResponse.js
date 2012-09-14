var OffsetsResponse = require('../lib/offsetsResponse');
var should = require('should');
var bignum = require('bignum');
var BufferMaker = require('BufferMaker');

describe("OffsetsResponse", function() {
  it ("should have an offsets property", function(){
    var res = new OffsetsResponse([bignum("1234123412341234132412341234"), bignum("0")]);
    res.offsets.length.should.equal(2);
    res.offsets[0].toString().should.equal("1234123412341234132412341234");
  });

  describe("parseFrom", function(){
    var binaryResponse = new BufferMaker()
                            .UInt32BE(22)   // response length
                            .UInt16BE(0)  // error code
                            .UInt32BE(2)    // number of offsets
                            .Int64BE(0)   // offset 1
                            .Int64BE(23)   // offset 23
                            .make();
    var res = OffsetsResponse.parseFrom(binaryResponse);
    res.offsets.length.should.equal(2);
    res.offsets[0].eq(0).should.equal(true);
    res.offsets[1].eq(23).should.equal(true);
  });


});
