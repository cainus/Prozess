require('should');
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
      var message = new Message("foobar");
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
});
