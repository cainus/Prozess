var should      = require('should');
var _           = require('underscore');
var binary      = require('binary');
var BufferMaker = require('buffermaker');
var Message     = require('../index').Message;

describe("Message", function(){
  beforeEach(function(){
    this.message = new Message('');
  });
  describe("ctor", function(){

    it("should have a default magic number", function(){
      this.message.MAGIC_IDENTIFIER_DEFAULT.should.equal(1);
    });

    it("should have a checksum and a payload", function(){
      this.message.payload.should.eql('');
      this.message.checksum.should.equal(0);
    });

    it("should set a default value of one", function(){
      this.message.magic.should
      .equal(this.message.MAGIC_IDENTIFIER_DEFAULT);
    });

    it("should allow to set a custom magic number", function(){
      this.message = new Message("ale", 0, 1, 0);
      this.message.magic.should.equal(1);
    });

    it("should have a default compression type of 0", function(){
      this.message = new Message("ale", 0);
      this.message.compression.should.equal(0);
    });

    it("should allow to set a compression type", function(){
      this.message = new Message("ale", 0, 1, 2);
      this.message.compression.should.equal(2);
    });

    it("should throw an error if payload isn't set", function(){
      try {
        var message = new Message();
        should.fail("expected exception was not raised");
      } catch(ex){
        ex.should.equal('payload is a required argument');
      }
    });

    it("should throw an exception for an invalid compression type", function(){
      try {
        var INVALID_COMPRESSION_TYPE = 99;
        this.message = Message('payload', 0, 1, INVALID_COMPRESSION_TYPE);
        should.fail('ctor should not allow Messages to be constructed with bad compression types');
      } catch(ex) {
        true.should.equal(ex.toString().search('InvalidCompressionType:') > -1);
      }
    });

  });

  describe(".byteLength", function(){
    var fillBufferWithValidContents = function(buffer) {
      buffer.writeUInt32BE(8, 0);          // size
      buffer.writeUInt8(1, 4);              // magic
      buffer.writeUInt8(1, 5);              // compression
      buffer.writeUInt32BE(755095536, 6);   // checksum
      buffer.write("foo", 10);
    };
    it("should return the length of the Message object in bytes", function(){
      var expectedSize = 12;
      var bytes = new Buffer(expectedSize);
      fillBufferWithValidContents(bytes);
      Message.fromBytes(bytes).byteLength().should.equal(expectedSize);
    });
  });

  describe("#calculateChecksum", function(){

    it("should calculate the checksum (crc32 of a given message)", function(){
      this.message.payload = "ale";
      this.message.calculateChecksum().should.equal(1120192889);
      this.message.payload = "alejandro";
      this.message.calculateChecksum().should.equal(2865078607);
    });
    it("should calculate the checksum even with ümlauts", function(){
      this.message.payload = "ümlaut";
      this.message.calculateChecksum().should.equal(1375136672);
    });

  });


  describe("#isValid", function(){

    it("should say if the message is valid using the crc32 signature", function(){
      this.message.payload  = "alejandro";
      this.message.checksum = 2865078607;
      this.message.isValid().should.equal(true);
      this.message.checksum = 0;
      this.message.isValid().should.equal(false);
      this.message = new Message("alejandro", 1337, 0, 0);
      this.message.isValid().should.equal(false);
    });

  });

  describe("#toBytes", function(){
    it("should encode a  >= 0.7 message", function(){
      var message = new Message("kafkatest");
      var fullMessage = new BufferMaker()
      .UInt32BE(15)
      .UInt8(message.magic)
      .UInt8(message.compression)
      .UInt32BE(message.calculateChecksum())
      .string(message.payload)
      .make();
      message.toBytes().should.eql(fullMessage);

    });

    it("should encode an empty >= 0.7 message", function(){
      var message = new Message("");
      var fullMessage = new BufferMaker()
      .UInt32BE(6)
      .UInt8(message.magic)
      .UInt8(message.compression)
      .UInt32BE(message.calculateChecksum())
      .string(message.payload)
      .make();
      message.toBytes().should.eql(fullMessage);
    });

    it("should encode strings containing non-ASCII characters", function(){

      var message = new Message("ümlaut");
      var fullMessage = new BufferMaker()
      .UInt32BE(13)
      .UInt8(message.magic)
      .UInt8(message.compression)
      .UInt32BE(message.calculateChecksum())
      .string(message.payload)
      .make();
      var bytes = message.toBytes();
      bytes.should.eql(fullMessage);
    });
  });

  describe("#fromBytes", function(){

    it("should parse a message when magic is 0 (kafka <= 0.6)", function(){
      var bytes = new BufferMaker()
      .UInt32BE(8) //size
      .UInt8(0) //magic
      .UInt32BE(1120192889)//checksum
      .string("ale")
      .make();
      var message = Message.fromBytes(bytes);
      message.magic.should.equal(0);
      message.checksum.should.equal(1120192889);
      message.payload.toString().should.equal("ale", 'utf8');
      message.isValid().should.equal(true);
    });

    it ("throws an exception if it can't get size", function(){
      try {
        var message = Message.fromBytes(new Buffer([0,1]));
        should.fail("expected exception was not thrown!");
      } catch (ex){
         ex.should.equal("incomplete message");
      }
    });

    it("should parse a message when magic is 1  (kafka >= 0.7)", function(){
      var bytes = new BufferMaker()
      .UInt32BE(9) //size
      .UInt8(1) // magic
      .UInt8(0) // compression
      .UInt32BE(1120192889) // checksum
      .string("ale")
      .make();
      var message = Message.fromBytes(bytes);
      message.magic.should.equal(1);
      message.checksum.should.equal(1120192889);
      message.isValid().should.equal(true);
      message.payload.toString().should.equal("ale", 'utf8');
    });

    it("should raise an error if the magic number is not recognised", function(){
      var bytes = new Buffer(13);
      bytes.writeUInt32BE(8, 0);          // size
      bytes.writeUInt8(2, 4);              // magic
      bytes.writeUInt8(1, 5);              // compression
      bytes.writeUInt32BE(755095536, 6);   // checksum
      bytes.write("ale", 10);
      try {
        Message.fromBytes(bytes);
      } catch(err) {
        err.should.equal("Unsupported Kafka message version: 2"); 
      }
    });

    it("should raise an exception on an incomplete 0.6 message", function(){
      var bytes = new BufferMaker()
      .UInt32BE(8)
      .UInt8(0)
      .UInt32BE(1120192889)
      .string("al")
      .make();

      try {
        var message= Message.fromBytes(bytes);
        should.fail("expected exception was not raised.");
      } catch(ex){
        ex.should.equal("incomplete message");
      }
    });

    it("should raise an exception on an incomplete 0.7 message", function(){
      var bytes = new BufferMaker()
      .UInt32BE(9)
      .UInt8(1)
      .UInt8(0)
      .UInt32BE(1120192889)
      .string("al")
      .make();

      try {
        var message= Message.fromBytes(bytes);
        should.fail("expected exception was not raised.");
      } catch(ex){
        ex.should.equal("incomplete message");
      }
    });

    it("should raise an exception on an incomplete message (no body) ", function(){
      var bytes = new BufferMaker(23)
      .UInt32BE(8)           // size
      .UInt8(1)              // magic
      .UInt8(0)              // compression
      .UInt32BE(755095536)   // checksum
      // skipping message body here
      .make();   
      try {
        var message= Message.fromBytes(bytes);
        should.fail("expected exception was not raised.");
      } catch(ex){
        ex.should.equal("incomplete message");
      }
    });

    it("should read empty messages correctly", function(){ 
      var bytes = new BufferMaker()
      .UInt32BE(6)
      .UInt8(1)
      .UInt8(0)
      .UInt32BE(0)
      .string("")
      .make();
      var message = Message.fromBytes(bytes);
      message.payload.toString().should.equal("");
    });

    it("should parse a gzip-compressed message", function() {
      // compressed = 'H4sIAG0LI1AAA2NgYBBkZBB/9XN7YlJRYnJiCogCAH9lueQVAAAA'.unpack('m*').shift
      // bytes = [45, 1, 1, 1303540914, compressed].pack('NCCNa*')
      // message = Message.parse_from(bytes).messages.first
      // message.should be_valid
      // message.payload.should == 'abracadabra'
    });


  });

});
