require('should');
require('underscore');
var binary = require('binary');
var BufferMaker = require('buffermaker');
var Message = require('../lib/message').Message;

describe("Message", function(){
  beforeEach(function(){
    this.message = new Message();
  });

  it("should have a default magic number", function(){
    this.message.MAGIC_IDENTIFIER_DEFAULT.should.equal(1);
  });

  it("should have a checksum and a payload", function(){
    this.message.payload.should.eql('');
    this.message.checksum.should.equal(0);
  });

  it("should set a default value of one", function(){
    console.log(this.message);
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
    this.message = new Message("alejandro", 1337, 0, 0);
    this.message.isValid().should.equal(false);
  });

  it("should parse a message when magic is 0 (kafka <= 0.6)", function(){
    var bytes = new BufferMaker()
              .UInt32BE(8) //size
              .UInt8(0) //magic
              .UInt32BE(1120192889)//checksum
              .string("ale")
              .make();
    var messageSet = Message.parseFrom(bytes);
    var message = messageSet.messages[0];
    message.magic.should.equal(0);
    message.checksum.should.equal(1120192889);
    message.isValid().should.equal(true);
    message.payload.toString().should.equal("ale", 'utf8');
  });
  
  it("should parse a message when magic is 1  (kafka >= 0.7)", function(){
    var bytes = new BufferMaker()
              .UInt32BE(9) //size
              .UInt8(1) // magic
              .UInt8(0) // compression
              .UInt32BE(1120192889) // checksum
              .string("ale")
              .make();
    var message = Message.parseFrom(bytes).messages[0];
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
     (function(){
       Message.parseFrom(bytes);
     }).should.throwError(/Unsupported Kafka message version/); 
   });

   it("should skip an incomplete 0.6 message at the end of the response", function(){
    var bytes = new BufferMaker()
              .UInt32BE(8)
              .UInt8(0)
              .UInt32BE(1120192889)
              .string("ale")
              .UInt32BE(8)
              .make();

     var messageSet = Message.parseFrom(bytes);
     messageSet.messages.length.should.equal(1);
     messageSet.size.should.equal(12); // bytes consumed
   });

   it("should skip an incomplete 0.7 message at the end of the response", function(){
    var bytes = new BufferMaker()
              .UInt32BE(9)
              .UInt8(1)
              .UInt8(0)
              .UInt32BE(1120192889)
              .string("ale")
              .UInt32BE(8)
              .make();

     var messageSet = Message.parseFrom(bytes);
     messageSet.messages.length.should.equal(1);
     messageSet.size.should.equal(13); // bytes consumed
   });

   it("should skip an incomplete message at the end of the response which has the same length as an empty message", function(){
     var bytes = new Buffer(23);
     bytes.writeUInt32BE(8, 0);          // size
     bytes.writeUInt8(1, 4);              // magic
     bytes.writeUInt8(0, 5);              // compression
     bytes.writeUInt32BE(755095536, 6);   // checksum
     bytes.write("ale", 10);
     bytes.writeUInt32BE(8, 13); // incomplete message (only length, rest is truncated)
     bytes.writeUInt8(1,17);
     bytes.writeUInt8(0, 18);
     bytes.writeUInt32BE(755095536,19);
     var messageSet = Message.parseFrom(bytes);
     messageSet.messages.length.should.equal(1);
     messageSet.size.should.equal(12); // bytes consumed
   });
  
   it("should read empty messages correctly", function(){ 
    var bytes = new BufferMaker()
              .UInt32BE(6)
              .UInt8(1)
              .UInt8(0)
              .UInt32BE(0)
              .string("")
              .make();
     var messageSet = Message.parseFrom(bytes);
     messageSet.messages.length.should.equal(1);
     messageSet.messages[0].payload.toString().should.equal("");
   });

   it("should parse a gzip-compressed message", function() {
    

     // compressed = 'H4sIAG0LI1AAA2NgYBBkZBB/9XN7YlJRYnJiCogCAH9lueQVAAAA'.unpack('m*').shift
     // bytes = [45, 1, 1, 1303540914, compressed].pack('NCCNa*')
     // message = Message.parse_from(bytes).messages.first
     // message.should be_valid
     // message.payload.should == 'abracadabra'
   });

   it("should recursively parse nested uncompressed messages", function() {

     var bytes = new BufferMaker()
     .UInt32BE(9)
     .UInt8(1)
     .UInt8(0)
     .UInt32BE(1120192889)
     .string("ale")
     .UInt32BE(12)
     .UInt8(1)
     .UInt8(0)
     .UInt32BE(2666930069)
     .string("foobar")
     .make();
     var messageSet = Message.parseFrom(bytes);
     messageSet.messages.length.should.equal(2);
     messageSet.messages[0].isValid().should.equal(true);
     messageSet.messages[0].payload.toString().should.equal("ale", "utf8");
     messageSet.messages[1].isValid().should.equal(true);
     messageSet.messages[1].payload.toString().should.equal("foobar", "utf8");
   });


});
