var should = require('should');
var net = require('net');
var bignum = require('bignum');
var _ = require('underscore');
var Consumer = require('../lib/Consumer');
var Message = require('../lib/Message');
var FetchResponse = require('../lib/FetchResponse');
var OffsetsResponse = require('../lib/OffsetsResponse');
var BufferMaker = require('buffermaker');
var binary = require('binary');


function closeServer(server, cb){
  if (!!server){
    try {
      server.close(function(){
        cb();
      });
      server = null;
      setTimeout(function(){cb();}, 1000);
    } catch(ex){
        server = null;
        cb();
    }

  } else {
    cb();
  }
}



describe("Consumer", function(){
  beforeEach(function(done){
    if (!this.server){  this.server = null; }
    closeServer(this.server, done);
  });
  //afterEach(function(done){
  //  closeServer(this.server, done);
  //});

  describe("#ctor", function(){

    it("creates a consumer that knows about the fetch message type", function(){
      var consumer = new Consumer();
      consumer.RequestType.FETCH.should.equal(1);
    });

    it("creates a consumer with a default topic", function(){
      var consumer = new Consumer();
      consumer.topic.should.equal('test');
    });

    it("creates a consumer withi a default partition", function(){
      var consumer = new Consumer();
      consumer.partition.should.equal(0);
    });

    it("creates a consumer with a default polling time", function(){
      var consumer = new Consumer();
      consumer.polling.should.equal(2);
    });

    it("creates a consumer with a passed in topic", function(){
      var consumer = new Consumer({topic:"newtopic"});
      consumer.topic.should.equal("newtopic");
    });

    it("creates a consumer with a passed in partition", function(){
      var consumer = new Consumer({partition:3});
      consumer.partition.should.equal(3);
    });

    it("creates a consumer with a default host of 'localhost'", function(){
      var consumer = new Consumer();
      consumer.host.should.equal('localhost');
    });

    it("creates a consumer with a default port of 9092", function(){
      var consumer = new Consumer();
      consumer.port.should.equal(9092);
    });

    it("creates a consumer with a default offset of null", function(){
      var consumer = new Consumer();
      should.not.exist(consumer.offset);
    });

    it("creates a consumer with a passed in offset", function(){
      var consumer = new Consumer({offset:1111});
      consumer.offset.should.equal(1111);
    });

    it("creates a consumer with a maximum message size", function(){
      var consumer = new Consumer();
      consumer.maxMessageSize.should.equal(1048576);
    });

  });


  describe("#sendConsumeRequest", function(){
    it("should send a consumer request", function(done){
      var consumer = new Consumer({ port : 9092});
      this.server = net.createServer(function(listener){
        listener.on('data', function(data){
          console.log("got some dater");
          var expected = "00 00 00 18 00 01 00 04 74 65 73 74 00 00 00 00 00 00 00 00 00 00 00 00 00 10 00 00";
          expected = expected.split(" ");
          expected = _.map(expected, function(datum){
            return parseInt(datum, 16);
          });
          data.should.eql(new Buffer(expected));
          console.log("writing back to consumer...");
          var fetchResponse = new FetchResponse(0, [new Message("ale"), new Message("foobar")]);
          listener.write(fetchResponse.toBytes());
          console.log("got this far.  I'm awesome");
        });

      });

      var server = this.server;
      this.server.listen(9092, function(){
        consumer.connect(function(err){
          console.log("connect connected");
          consumer.offset = bignum(0);
          consumer.sendConsumeRequest(function(err, fetchResponse){
            if (!!err){console.log(err);}
            should.not.exist(err);
            console.log(fetchResponse);
            fetchResponse.error.should.equal(0);
            fetchResponse.messages.length.should.equal(2);
            fetchResponse.messages[0].payload.toString().should.equal("ale");
            if (!!err){throw err;}
            done();
          });
        });

      });
    });
  });


  describe("#getOffsets", function(){

    it("should send an offset request and give a response object" , function(done){
     this.server = net.createServer(function(listener){
       listener.on('data', function(data){
         // TODO validate the incoming offsets request

         // create a response
         var binaryResponse = new BufferMaker()
         .UInt32BE(22)   // response length
         .UInt16BE(0)  // error code
         .UInt32BE(2)    // number of offsets
         .Int64BE(0)   // offset 1
         .Int64BE(23)   // offset 23
         .make();

         listener.write(binaryResponse);
       });

     });

     this.server.listen(9092, function(){
     });

     var consumer = new Consumer();
     consumer.connect(function(err){
       consumer.getOffsets(function(err, offsetsResponse){
         console.log("err: ", err);
         if (err) { throw err; }
         console.log("got here!!");
         if (offsetsResponse.error) { throw offsetsResponse.error; }
         var offsets = offsetsResponse.offsets;
         offsets.length.should.equal(2);
         offsets[1].eq(23).should.equal(true);
         done();
       });
     });

    });

    it("sends offset requests, gather the response OVER TIME, and gives response objects" , function(done){
     this.server = net.createServer(function(listener){
       listener.on('data', function(data){
         // TODO validate the incoming offsets request

         // create a response
         var binaryResponse = new BufferMaker()
         .UInt32BE(22)   // response length
         .UInt16BE(0)  // error code
         .UInt32BE(2)    // number of offsets
         .make();
         listener.write(binaryResponse);

         setTimeout(function(){
           binaryResponse = new BufferMaker()
           .Int64BE(54)   // offset 1
           .Int64BE(23)   // offset 23
           .make();

           listener.write(binaryResponse);
         }, 120);  // 120 ms delay on second half
       });

     });

     this.server.listen(9092, function(){
     });

     var consumer = new Consumer();
     consumer.connect(function(err){
       consumer.getOffsets(function(err, offsetsResponse){
         if (err) { throw err; }
         var offsets = offsetsResponse.offsets;
         offsets.length.should.equal(2);
         offsets[1].eq(23).should.equal(true);
         console.log("OFFSET: ", consumer.offset);
         done();
       });
     });

    });

  });

  describe("#consume", function(){

    it ("should consume with the latest offset if no offset is provided", function(done){
       this.server = net.createServer(function(listener){
         var requestBuffer = new Buffer([]);

         listener.on('data', function(data){
           console.log("got data: ", data);
           requestBuffer = Buffer.concat([requestBuffer, data]);
           data = requestBuffer;
           // check if it's a full request:

           var unpacked = binary(requestBuffer)
                            .word32bu("length")
                            .word16bu("type")
                            .vars;

           console.log("unpacked: ", unpacked);
           console.log("data.length: ", data.length);
           var totalExpectedLength = unpacked.length + 4;  // 4 bytes for the length field
           if (data.length >= totalExpectedLength ){
             requestBuffer = requestBuffer.slice(totalExpectedLength);
             console.log("requestBuffer truncated to: ", requestBuffer);

             switch(unpacked.type){
               case 4 :  // OFFSETS
                   // fake offsets response!
                   console.log("step 4. writing fetch response");
                   var offsetsResponse = new OffsetsResponse(0, [54, 23]);
                   listener.write(offsetsResponse.toBytes());
                   break;


                case 1:  // MESSAGES
                  console.log("step 5. writing a messages response");
                  // fake messages response!

                  var fetchResponse = new FetchResponse(0, [new Message("ale"), new Message("foobar")]);
                  console.log("writing: ", fetchResponse);
                  console.log("which is: ", fetchResponse.toBytes());

                  listener.write(fetchResponse.toBytes());
                  break;
                default : throw "unknown request type: " + unpacked.type;
             }
           }
         });

       });

       var consumer = new Consumer({topic : "test"});
       console.log("step 1. created consumer");
       this.server.listen(9092, function(){
         console.log("step 2. server listening");
         consumer.connect(function(err){
           console.log("step 3. client connected");
           console.log("This test connected");
           consumer.consume(function(err, messages){
             console.log("CONSUME CB input: ", err, messages);
             should.not.exist(err);
             messages.length.should.equal(2);
             messages[0].payload.toString().should.equal("ale");
             console.log("consumer.offset", consumer.offset);
             (consumer.offset.eq(89)).should.equal(true);
             done();
           });
         });
       });


      });
  });


});

