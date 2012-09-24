var should = require('should');
var binary = require('binary');
var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var Protocol = require('../lib/Protocol');


describe("Protocol", function(){

  describe("#encodeOffsetsRequests", function(){
    it("should fetch the latest offset" , function(){
      var expectedBytes = new BufferMaker()
      .UInt8(0)
      .UInt8(0)
      .UInt8(0)
      .UInt8(24) // request length
      .UInt8(0)
      .UInt8(4)  // request type
      .UInt8(0)
      .UInt8(4)  // topic length
      .string('test')
      .Int32BE(3)  // partition
      .string(new Buffer([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]))
      .UInt32BE(1)  // max # of offsets
      .make();
      var protocol = new Protocol("test", 3);
      var request = protocol.encodeOffsetsRequest(-1, 1);
      request.should.eql(expectedBytes);
    });
  });







});
