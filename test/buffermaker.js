require('should');
var BufferMaker = require('../lib/buffermaker').BufferMaker;

describe("BufferMaker", function(){
  it("should create a one byte buffer", function(){
    var buffer = new Buffer(1);
    buffer.writeUInt8(5,0);
    var actual = new BufferMaker().UInt8(5).make();
    console.log(actual);
    actual.should.eql(buffer);
  });
});
