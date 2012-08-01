require('should');
var binary = require('binary');

var Message = require('../lib/message').Message;

describe("Message", function(){
  beforeEach(function(){
    this.message = new Message();
  });

  it("should have a default magic number", function(){
    this.message.MAGIC_IDENTIFIER_DEFAULT.should.equal(0);
  });

  it("should have a checksum and a payload", function(){
    this.message.payload.should.eql(new Buffer([]));
    this.message.checksum.should.equal(0);
  });

  it("should set a default value of zero", function(){
    this.message.magic.should
          .equal(this.message.MAGIC_IDENTIFIER_DEFAULT);
  });

  it("should allow to set a custom magic number", function(){
      this.message = new Message("ale", 1);
      this.message.magic.should.equal(1);
  });

  it("should calculate the checksum (crc32 of a given message)", function(){
    this.message.payload = "ale";
    this.message.calculateChecksum().should.equal(1120192889);
    this.message.payload = "alejandro";
    this.message.calculateChecksum().should.equal(2865078607);
  });

  it("should say if the message is valid using the crc32 signature", function(){
    this.message.payload  = "alejandro";
    this.message.checksum = 2865078607;
    this.message.isValid().should.equal(true);
    this.message.checksum = 0;
    this.message.isValid().should.equal(false);
    this.message = new Message("prozess", 0, 1337);
    this.message.isValid().should.equal(false);
  });

    it("should parse a message from bytes", function(){
      //var bytes = [12].pack("N") 
      //              + [0].pack("C") 
      //              + [558161692].pack("N") + "proz";
      var bytes = new Buffer(13);
      console.log("buffer: ", bytes, " L:", bytes.length);
      bytes.writeUInt32BE(12, 0);
      console.log("buffer: ", bytes, " L:", bytes.length);
      bytes.writeUInt8(0, 4);
      console.log("buffer: ", bytes, " L:", bytes.length);
      bytes.writeUInt32BE(1120192889, 5);
      console.log("buffer: ", bytes, " L:", bytes.length);
      bytes.write("ale", 9);
      console.log("buffer: ", bytes, " L:", bytes.length);
      var message = Message.parseFrom(bytes);
      message.isValid().should.equal(true);
      message.magic.should.equal(0);
      message.checksum.should.equal(1120192889);
      console.log(message.payload.toString('utf8'));
      console.log(message.payload);
      message.payload.toString().should.equal("ale", 'utf8');
    });


});
