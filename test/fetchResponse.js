var FetchResponse = require('../lib/fetchResponse').FetchResponse;
var _ = require('underscore');
require('should');


function bufferFromString(str){
    var bytes = str.split(" ");
    bytes = _.map(bytes, function(datum){
      return parseInt(datum, 16);
    });
    bytes = new Buffer(bytes);
    return bytes;
}


describe("FetchResponse", function(){

//  beforeEach(function(){
//    this.oneMessage = bufferFromString("00 00 00 2a 00 00 00 00 00 10 00 eb 8e 66 87 48 65 6c 6c 6f 20 74 68 65 72 65");
//    this.twoMessage = bufferFromString("00 00 00 2a 00 00 00 00 00 10 00 eb 8e 66 87 48 65 6c 6c 6f 20 74 68 65 72 65 00 00 00 10 00 7c 3a c8 85 48 65 6c 6c 6f 20 74 68 21 21 21");
//  });
//
//
//  describe("#ctor", function(){
//    it("should have a length property", function(){
//      var response = FetchResponse.parseFrom(this.oneMessage);
//      response.length.should.equal(42);
//    });
//
//    it("should have a error property", function(){
//      var response = FetchResponse.parseFrom(this.oneMessage);
//      response.error.should.equal(0);
//    });
//
//    it ("should parse a 1-message body", function(){
//      var response = FetchResponse.parseFrom(this.oneMessage);
//      response.messages.length.should.equal(1);
//      response.messages[0].payload.toString('utf8').should.equal("Hello there");
//    });
//
//    it ("should parse a 2-message body", function(){
//      var response = FetchResponse.parseFrom(this.twoMessage);
//      response.messages.length.should.equal(2);
//      console.log(response.messages[0].payload);
//      response.messages[0].payload.toString('utf8').should.equal("Hello there");
//    });
//  });


});




