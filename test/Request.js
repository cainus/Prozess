var Request = require('../index').Request;
var should = require('should');
var binary = require('binary');
var BufferMaker = require('buffermaker');

describe("Request", function() {
  describe("toBytes", function() {
    it ("should return a buffer representing a request", function(){
      var PARTITION = 1;
      var request = new Request("topic", PARTITION, Request.Types.MULTIPRODUCE, new Buffer('the body'));
      var bytes = request.toBytes();
      var actual = binary.parse(bytes)
                          .word32bu('length')
                          .word16bu('type')
                          .word16bu('topicLength')
                          .buffer('topic', 'topic'.length)
                          .word32bu('partition')
                          .vars;
      actual.length.should.equal(21);
      actual.type.should.equal(Request.Types.MULTIPRODUCE);
      actual.topicLength.should.equal("topic".length);
      actual.topic.toString().should.equal('topic');
      actual.partition.should.equal(PARTITION);
    });
  });
  describe("fromBytes", function() {
    it("should return a Request from a Buffer", function() { 
      var PARTITION = 3;
       var input = new BufferMaker()
        .UInt32BE(2 + 2 + 'topic'.length + 4 + 'body'.length) 
        .UInt16BE(Request.Types.FETCH)
        .UInt16BE('topic'.length)
        .string('topic')
        .UInt32BE(PARTITION)
        .string('body')
        .make();
       var actual = Request.fromBytes(input);
       actual.topic.should.equal('topic');
       actual.partition.should.equal(PARTITION);
       actual.type.should.equal(Request.Types.FETCH);
       actual.body.toString().should.equal('body');
    });
  });
});
