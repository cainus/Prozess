var should = require('should');
var binary = require('binary');
var BufferMaker = require('buffermaker');
var Message = require('../lib/Message');
var ProduceRequest = require('../lib/ProduceRequest');

describe("ProduceRequest", function(){
    describe("#toBytes", function(){
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
        var request = new ProduceRequest('test', 0, []);
        var bytes = request.toBytes();
        bytes.length.should.equal(20);
        bytes.should.eql(fullRequest);
      }); 

    });


    it("should binary encode a request with a message, using a specific wire format", function(){
      var message = new Message("ale");

      var encodedRequest;
      try {
        encodedRequest = new ProduceRequest('test', 0, [message]).toBytes();
      } catch (ex){
        console.log("EX!: ", ex);
      }


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
