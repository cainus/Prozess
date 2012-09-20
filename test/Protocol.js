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


  describe("#encodeOffsetsResponse", function(){
    it ("should encode an offsets response from 2 offsets", function(){
      var expected = new BufferMaker()
                   .UInt32BE(22)   // response length
                   .UInt16BE(0)  // error code
                   .UInt32BE(2)    // number of offsets
                   .Int64BE(54)   // offset 1
                   .Int64BE(23)   // offset 23
                   .make();
      new Protocol().encodeOffsetsResponse([54, 23]).should.eql(expected);
    }); 
  
  });




  describe("#encodeFetchRequest", function(){
    it("should encode a request to consume", function(){
      var protocol = new Protocol("test", 0, 14);
      var bytes = new BufferMaker()
      .UInt32BE(24)      // the size of all of it
      .UInt16BE(protocol.RequestTypes.FETCH)
      .UInt16BE(protocol.topic.length)
      .string("test")
      .UInt32BE(0)
      .UInt32BE(0)
      .UInt32BE(0)
      .UInt32BE(protocol.maxMessageSize)
      .make();


      //var bytes = new Buffer(24);

      protocol.encodeFetchRequest(0).should.eql(bytes);
    });
});

});
