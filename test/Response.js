var Response = require('../lib/Response');
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

  it ("should parse a response from binary", function(){
    var data = bufferFromString("00 00 00 25 00 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00");
    var response = Response.fromBytes(data);
    response.error.should.equal(1);
  
  });

});
