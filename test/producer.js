require('should');
var binary = require('binary');
var net = require('net');
var Producer = require('../lib/producer').Producer;
var BufferMaker = require('buffermaker');
var Message = require('../lib/message').Message;


describe("Producer", function(){
  beforeEach(function(){
    this.producer = new Producer();
  });

  describe("Kafka Producer", function(){


    it("should have a request type id", function(){
      this.producer.requestType.should.equal(0);
    });

    it("should have a default topic id", function(){
      this.producer.topic.should.equal('test');
    }); 

    it("should have a default partition", function(){
      this.producer.partition.should.equal(0);
    });

    it("should have a default host", function(){
      this.producer.host.should.equal('localhost');
    });

    it("should have a default port", function(){
      this.producer.port.should.equal(9092);
    });

  });

  describe("Message Encoding", function(){
    it("should encode a  >= 0.7 message", function(){
      var message = new Message("kafkatest");
      var fullMessage = new BufferMaker()
      .UInt8(message.magic)
      .UInt8(message.compression)
      .UInt32BE(message.calculateChecksum())
      .string(message.payload)
      .make();
      var producer = new Producer();
      producer.encode(message).should.eql(fullMessage);

    });

    it("should encode an empty >= 0.7 message", function(){
      var message = new Message("");
      var fullMessage = new BufferMaker()
      .UInt8(message.magic)
      .UInt8(message.compression)
      .UInt32BE(message.calculateChecksum())
      .string(message.payload)
      .make();
      var producer = new Producer();
      producer.encode(message).should.eql(fullMessage);
    });

    it("should encode strings containing non-ASCII characters", function(){

      var message = new Message("ümlaut");
      var fullMessage = new BufferMaker()
      .UInt8(message.magic)
      .UInt8(message.compression)
      .UInt32BE(message.calculateChecksum())
      .string(message.payload)
      .make();
      var producer = new Producer();
      producer.encode(message).should.eql(fullMessage);
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
          var producer = new Producer();
          var request = producer.encodeRequest([]);
          request.length.should.equal(20);
          request.should.eql(fullRequest);
        }); 

      });


      it("should binary encode a request with a message, using a specific wire format", function(){
        var producer = new Producer();
        var message = new Message("ale");

        var encodedRequest = producer.encodeRequest([message]);
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
