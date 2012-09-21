var FetchRequest = require('../lib/FetchRequest');
var BufferMaker = require('buffermaker');

var should = require('should');
describe("#toBytes", function(){
  it("should encode a request to consume", function(){
    var fetchRequest = new FetchRequest(33,"test", 0, 10 * 1024 * 1024);
    var bytes = new BufferMaker()
    .UInt32BE(24)      // the size of all of it
    .UInt16BE(fetchRequest.RequestTypes.FETCH)
    .UInt16BE(fetchRequest.topic.length)
    .string("test") //topic
    .UInt32BE(0) // partition
    .UInt32BE(0) // offset first 32 
    .UInt32BE(33) // offset second 32
    .UInt32BE(10 * 1024 * 1024)
    .make();

    var encodedFetchRequest = fetchRequest.toBytes();
    encodedFetchRequest.should.eql(bytes);
  });
});
