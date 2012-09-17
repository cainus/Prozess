var should = require('should');
var net = require('net');
var bignum = require('bignum');
var _ = require('underscore');
var Consumer = require('../lib/consumer');
var BufferMaker = require('BufferMaker');
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
  afterEach(function(done){
    closeServer(this.server, done);
  });

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

    it("creates a consumer withshould have a maximum message size", function(){
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
          listener.write('some data');
          console.log("got this far.  I'm awesome");
        });

      });

      var server = this.server;
      this.server.listen(9092, function(){
        consumer.connect(function(err){
          console.log("connect connected");
          consumer.sendConsumeRequest(0, function(err, messageSet){
            console.log("sent consume request");
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
         // TODO validate the incoming offets request

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
       consumer.getOffsets(function(err, offsets){
         if (err) { throw err; }
         offsets.length.should.equal(2);
         offsets[1].eq(23).should.equal(true);
         done();
       });
     });

    });

    it("should send an offset request OVER TIME and give a response object" , function(done){
     this.server = net.createServer(function(listener){
       listener.on('data', function(data){
         // TODO validate the incoming offets request

         // create a response
         var binaryResponse = new BufferMaker()
         .UInt32BE(22)   // response length
         .UInt16BE(0)  // error code
         .UInt32BE(2)    // number of offsets
         .make();
         listener.write(binaryResponse);

         setTimeout(function(){
           binaryResponse = new BufferMaker()
           .Int64BE(0)   // offset 1
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
       consumer.getOffsets(function(err, offsets){
         if (err) { throw err; }
         offsets.length.should.equal(2);
         offsets[1].eq(23).should.equal(true);
         done();
       });
     });

    });

  });

  describe("#consume", function(){

    it ("should consume with the latest offset if no offset is provided", function(done){
       this.server = net.createServer(function(listener){
         var request = 0;
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
                   var fetchResponse = new BufferMaker()
                   .UInt32BE(22)   // response length
                   .UInt16BE(0)  // error code
                   .UInt32BE(2)    // number of offsets
                   .Int64BE(0)   // offset 1
                   .Int64BE(23)   // offset 23
                   .make();

                   listener.write(fetchResponse);
                   break;
                case 1:  // MESSAGES
                  console.log("step 5. writing a messages response");
                  // fake messages response!

                  var messagesResponse = new BufferMaker()
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

                  listener.write(messagesResponse);
                  break;
                default : throw "unknown request type: " + unpacked.type;
             }
           }
         });

       });

       var consumer = new Consumer();
       console.log("step 1. created consumer");
       this.server.listen(9092, function(){
         console.log("step 2. server listening");
         consumer.connect(function(err){
           console.log("step 3. client connected");
           console.log("This test connected");
           consumer.consume(function(err, messages){
             console.log(err, messages);
             should.not.exist(err);
             messages.length.should.equal(2);
             messages[0].payload.toString().should.equal("ale");
             done();
           });
         });
       });


      });
  });


});

