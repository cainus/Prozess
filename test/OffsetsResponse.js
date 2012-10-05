var OffsetsResponse = require('../index').OffsetsResponse;
var should = require('should');
var bignum = require('bignum');
var BufferMaker = require('buffermaker');

describe("OffsetsResponse", function() {
  it ("should have an offsets property", function(){
    var res = new OffsetsResponse(0, [bignum("1234123412341234132412341234"), bignum("0")]);
    res.offsets.length.should.equal(2);
    res.offsets[0].toString().should.equal("1234123412341234132412341234");
  });
  it ("should have an error property", function(){
    var res = new OffsetsResponse(0, [bignum("1234123412341234132412341234"), bignum("0")]);
    res.error.should.equal(0);
  });

/*
  <Buffer 
  00 00 00 1a 
  00 00 00 00 00 02 
  00 00 00 00 00 00 00 36 
  00 00 00 00 00 00 00 17> to equal <Buffer 
  
  00 00 00 16 
  00 00 00 00 00 02 
  00 00 00 00 00 00 00 36 
  00 00 00 00 00 00 00 17>
*/


  describe("#toBytes", function(){
    it ("should encode an offsets response from 2 offsets", function(){
      var expected = new BufferMaker()
                   .UInt32BE(22)   // response length
                   .UInt16BE(0)  // error code
                   .UInt32BE(2)    // number of offsets
                   .Int64BE(54)   // offset 1
                   .Int64BE(23)   // offset 23
                   .make();
      new OffsetsResponse(0, [54, 23]).toBytes().should.eql(expected);
    });
  });

  describe("#fromBytes", function(){
    it ("can parse two offsets out of a complete response", function(){
      var binaryResponse = new BufferMaker()
                              .UInt32BE(22) // response length
                              .UInt16BE(0)  // error code
                              .UInt32BE(2)  // number of offsets
                              .Int64BE(0)   // offset 1
                              .Int64BE(23)  // offset 23
                              .make();
      var res = OffsetsResponse.fromBytes(binaryResponse);
      res.error.should.equal(0);
      res.offsets.length.should.equal(2);
      res.offsets[0].eq(0).should.equal(true);
      res.offsets[1].eq(23).should.equal(true);
    });
    it ("throws an 'incomplete response' exception on incomplete responses", function(){
      var binaryResponse = new BufferMaker()
                              .UInt32BE(22) // response length
                              .UInt16BE(0)  // error code
                              .UInt32BE(2)  // number of offsets
                              .Int64BE(0)   // offset 1
                              // missing the second offset here.
                              .make();
      try {
        OffsetsResponse.fromBytes(binaryResponse);
        should.fail("expected exception was not raised");
      } catch (ex){ 
        ex.should.equal("incomplete response");
      }
    });
  });


});
