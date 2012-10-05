var FetchResponse = require('../index').FetchResponse;
var Message = require('../index').Message;
var _ = require('underscore');
var BufferMaker = require('buffermaker');
var should = require('should');


function bufferFromString(str){
    var bytes = str.split(" ");
    bytes = _.map(bytes, function(datum){
      return parseInt(datum, 16);
    });
    bytes = new Buffer(bytes);
    return bytes;
}


describe("FetchResponse", function(){
  describe("#toBytes", function(){
    it ("should encode a fetch response from 1 message", function(){
      var messageBytes = new Message("this is a test").toBytes();
      var expected = new BufferMaker()
                   .UInt32BE(26)  // response length = messages fields (24) + error field (2)
                   .UInt16BE(0)   // error code
                                  // only message
                   .string(messageBytes)   // messages
                   .make();
      var actual = new FetchResponse(0, [new Message("this is a test")]).toBytes();
      actual.should.eql(expected);

    });

    it ("should encode a fetch response from 2 messages", function(){
      var messageBytes = Buffer.concat([new Message("this is a test").toBytes(), new Message("so is this").toBytes()]);
      var expected = new BufferMaker()
                   .UInt32BE(46)  // response length = messages fields (44) + error field (2)
                   .UInt16BE(0)   // error code
                                  // only message
                   .string(messageBytes)   // messages
                   .make();
      (new FetchResponse(0, [new Message("this is a test"), new Message("so is this")]).toBytes()).should.eql(expected);

    });

    it ("should encode a fetch response from errors", function(){
      var expected = new BufferMaker()
                   .UInt32BE(2)  // response length = messages fields (44) + error field (2)
                   .UInt16BE(1)   // error code
                   .make();
      (new FetchResponse(1, []).toBytes()).should.eql(expected);

    });
  });

  describe("#fromBytes", function(){
    it ("should create a FetchResponse object from bytes", function(){
      var bytes = bufferFromString(
        "00 00 00 2e 00 00 00 00 00 14 01 00 0d 1e e7 ea 74 68 69 73 20 69 73 20 61 20 74 65 73 74 00 00 00 10 01 00 09 ab 7e a8 73 6f 20 69 73 20 74 68 69 73");
      bytes.should.eql(new FetchResponse(0, [new Message("this is a test"), new Message("so is this")]).toBytes());
      var res = FetchResponse.fromBytes(bytes);
      res.messages.length.should.equal(2);
    });

    it ("should handle a no-message response", function(){
      var bytes = bufferFromString("00 00 00 02 00 00");
      bytes.should.eql(new FetchResponse(0, []).toBytes());
      var res = FetchResponse.fromBytes(bytes);
      res.error.should.equal(0);
      res.messages.length.should.equal(0);
    });

    it ("should handle an error response", function(){
      var bytes = bufferFromString("00 00 00 02 00 01");
      bytes.should.eql(new FetchResponse(1, []).toBytes());
      var res = FetchResponse.fromBytes(bytes);
      res.error.should.equal(1);
      res.messages.length.should.equal(0);
    });


  });





//  beforeEach(function(){
//    this.oneMessage = bufferFromString("00 00 00 2a 00 00 00 00 00 10 00 eb 8e 66 87 48 65 6c 6c 6f 20 74 68 65 72 65");
//    this.twoMessage = bufferFromString("00 00 00 2a 00 00 00 00 00 10 00 eb 8e 66 87 48 65 6c 6c 6f 20 74 68 65 72 65 00 00 00 10 00 7c 3a c8 85 48 65 6c 6c 6f 20 74 68 21 21 21");
//  });
//
//
//  describe("#ctor", function(){
//    it("should have a length property", function(){
//      var response = FetchResponse.fromBytes(this.oneMessage);
//      response.length.should.equal(42);
//    });
//
//    it("should have a error property", function(){
//      var response = FetchResponse.fromBytes(this.oneMessage);
//      response.error.should.equal(0);
//    });
//
//    it ("should parse a 1-message body", function(){
//      var response = FetchResponse.fromBytes(this.oneMessage);
//      response.messages.length.should.equal(1);
//      response.messages[0].payload.toString('utf8').should.equal("Hello there");
//    });
//
//    it ("should parse a 2-message body", function(){
//      var response = FetchResponse.fromBytes(this.twoMessage);
//      response.messages.length.should.equal(2);
//      console.log(response.messages[0].payload);
//      response.messages[0].payload.toString('utf8').should.equal("Hello there");
//    });
//  });


});




