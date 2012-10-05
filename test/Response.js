var Response = require('../index').Response;
var FetchResponse = require('../index').FetchResponse;
var Message = require('../index').Message;
var should = require('should');
var bignum = require('bignum');
var _ = require('underscore');
var BufferMaker = require('buffermaker');


function bufferFromString(str){
    var bytes = str.split(" ");
    bytes = _.map(bytes, function(datum){
      return parseInt(datum, 16);
    });
    bytes = new Buffer(bytes);
    return bytes;
}

describe("Response", function() {
  it ("should have an errors object on it", function(){
    Response.Errors.Unknown.should.equal(-1);
    Response.Errors.NoError.should.equal(0);
    Response.Errors.OffsetOutOfRange.should.equal(1);
    Response.Errors.InvalidMessage.should.equal(2);
    Response.Errors.WrongPartition.should.equal(3);
    Response.Errors.InvalidFetchSize.should.equal(4);
  });

  describe("#toBytes", function(){
      var res = new Response(0, new Buffer([1,2,3,4]));
      res.toBytes().should.eql(new Buffer([0, 0, 0, 6, 0, 0, 1, 2, 3, 4]));
  });

  describe("#fromBytes", function(){
    it ("should create a Response object from bytes", function(){
      /*
        00 00 00 2e 00 00 00 00 00 14
        01 00 0d 1e e7 ea 74 68 69 73
        20 69 73 20 61 20 74 65 73 74
        00 00 00 10 01 00 09 ab 7e a8
        73 6f 20 69 73 20 74 68 69 73
      */
      var bytes = bufferFromString(
        "00 00 00 2e 00 00 00 00 00 14 01 00 0d 1e e7 ea 74 68 69 73 20 69 73 20 61 20 74 65 73 74 00 00 00 10 01 00 09 ab 7e a8 73 6f 20 69 73 20 74 68 69 73");
      bytes.should.eql(new FetchResponse(0, [new Message("this is a test"), new Message("so is this")]).toBytes());
      var res = Response.fromBytes(bytes);
      res.error.should.equal(0);
      res.body.length.should.eql(44);
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
        Response.fromBytes(binaryResponse);
        should.fail("expected exception was not raised");
      } catch (ex){ 
        ex.should.equal("incomplete response");
      }
    });

  });

});
