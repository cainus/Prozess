var should = require('should');
var BufferMaker = require('buffermaker');
var OffsetsRequest = require('../lib/OffsetsRequest');


describe("OffsetsRequest", function(){

  describe("#toBytes", function(){
    it("should create a buffer representing the request " , function(){
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
      .string(new Buffer([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]))  // -1
      .UInt32BE(1)  // max # of offsets
      .make();
      var topic = "test";
      var partition = 3;
      var since = -1;
      var maxNumber = 1;
      var request = new OffsetsRequest(topic, partition, since, maxNumber).toBytes();
      request.should.eql(expectedBytes);
    });
  });







});
