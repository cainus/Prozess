var should = require('should');
var binary = require('binary');
var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var Protocol = require('../lib/Protocol');


describe("Protocol", function(){
  describe("Message Encoding", function(){
    it("should encode a  >= 0.7 message", function(){
      var message = new Message("kafkatest");
      var fullMessage = new BufferMaker()
      .UInt8(message.magic)
      .UInt8(message.compression)
      .UInt32BE(message.calculateChecksum())
      .string(message.payload)
      .make();
      var protocol = new Protocol();
      protocol.encodeMessage(message).should.eql(fullMessage);

    });

    it("should encode an empty >= 0.7 message", function(){
      var message = new Message("");
      var fullMessage = new BufferMaker()
      .UInt8(message.magic)
      .UInt8(message.compression)
      .UInt32BE(message.calculateChecksum())
      .string(message.payload)
      .make();
      var protocol = new Protocol();
      protocol.encodeMessage(message).should.eql(fullMessage);
    });

    it("should encode strings containing non-ASCII characters", function(){

      var message = new Message("ümlaut");
      var fullMessage = new BufferMaker()
      .UInt8(message.magic)
      .UInt8(message.compression)
      .UInt32BE(message.calculateChecksum())
      .string(message.payload)
      .make();
      var protocol = new Protocol();
      protocol.encodeMessage(message).should.eql(fullMessage);
    });
  });

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

      describe("Request Encoding", function(){
        it("should binary encode an empty request", function() {
          var fullRequest = new BufferMaker()
                              .UInt8(0)
                              .UInt8(0)
                              .UInt8(0)
                              .UInt8(16)
                              .UInt8(0)
                              .UInt8(0)
                              .UInt8(0)
                              .UInt8(4)
                              .string('test')
                              .Int64BE(0)
                              .make();
          var protocol = new Protocol('test', 0);
          var request = protocol.encodeProduceRequest([]);
          request.length.should.equal(20);
          request.should.eql(fullRequest);
        }); 

      });


      it("should binary encode a request with a message, using a specific wire format", function(){
        var protocol = new Protocol('test', 0);
        var message = new Message("ale");

        var encodedRequest = protocol.encodeProduceRequest([message]);
        var unpacked = binary.parse(encodedRequest)
        .word32bu('dataSize')
        .word16bu('requestId')
        .word16bu('topicLength')
        .buffer('topic', 4)
        .word32bu('partition')
        .word32bu('messagesLength')
        .tap(function(vars){
          this.buffer('messages', vars.messagesLength);
        })
        .vars;

        encodedRequest.length.should.equal(33);
        unpacked.dataSize.should.eql(29);
        unpacked.requestId.should.eql(0);
        unpacked.topicLength.should.eql(4);
        unpacked.topic.toString('utf8').should.eql("test");
        unpacked.partition.should.eql(0);
       unpacked.messagesLength.should.eql(13);
      });
});
