var should = require('should');
var net = require('net');
var bignum = require('bignum');
var _ = require('underscore');
var Consumer = require('../lib/consumer');
var BufferMaker = require('buffermaker');


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

  describe("Kafka Consumer", function(){

    it("should have a Kafka::RequestType::FETCH", function(){
      var consumer = new Consumer();
      consumer.RequestType.FETCH.should.equal(1);
    });

    it("should have a default topic", function(){
      var consumer = new Consumer();
      consumer.topic.should.equal('test');
    });

    it("should have a default partition", function(){
      var consumer = new Consumer();
      consumer.partition.should.equal(0);
    });

    it("should have a default polling time", function(){
      var consumer = new Consumer();
      consumer.polling.should.equal(2);
    });

    it("should set a topic on initialize", function(){
      var consumer = new Consumer({topic:"newtopic"});
      consumer.topic.should.equal("newtopic");
    });
 
    it("should set a partition on initialize", function(){
      var consumer = new Consumer({partition:3});
      consumer.partition.should.equal(3);
    });

    it("should set default host if none is specified", function(){
      var consumer = new Consumer();
      consumer.host.should.equal('localhost');
    });

    it("should set a default port if none is specified", function(){
      var consumer = new Consumer();
      consumer.port.should.equal(9092);
    });
   
    it("should not have a default offset but be able to set it", function(){
      var consumer = new Consumer();
      //should.not.exist(consumer.offset);
      consumer = new Consumer({offset:1111});
      consumer.offset.should.equal(1111);
    });

    it("should have a max size", function(){
      var consumer = new Consumer();
      consumer.max_size.should.equal(1048576);
    });

    it("should return the size of the request", function(){
      var consumer = new Consumer();
      consumer.topic = "someothertopicname";
      console.log("someothertopic".length);
      console.log(consumer.encodedRequestSize());
      var buffer = new Buffer(4);
      buffer.writeUInt32BE(38, 0);
      consumer.encodedRequestSize().should.eql(buffer);
    });

    

    it("should encode a request to consume", function(){
      var consumer = new Consumer();
      var bytes = new BufferMaker()
      .UInt16BE(consumer.RequestType.FETCH)
      .UInt16BE(consumer.topic.length)
      .string("test")
      .UInt32BE(0)
      .UInt32BE(0)
      .UInt32BE(0)
      .UInt32BE(consumer.MAX_SIZE)
      .make();


      //var bytes = new Buffer(24);

      consumer.encodeRequest(consumer.RequestType.FETCH,
                                  "test",
                                  0,
                                  0,
                                  consumer.MAX_SIZE).should.eql(bytes);
    });





    describe("#sendConsumerRequest", function(){
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


    describe("getOffsets", function(){

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
         console.log("This test connected");
         consumer.getOffsets(function(err, offsets){
           offsets.length.should.equal(2);
           offsets[1].eq(23).should.equal(true);
           done();
         });
       });

      });

    });

    describe("encodeOffsetsRequests", function(){
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
        var consumer = new Consumer({ partition : 3 });
        var request = consumer.encodeOffsetsRequest(-1);
        request.should.eql(expectedBytes);
      });
    });

    it ("should consume with the latest offset if no offset is provided", function(done){
       this.server = net.createServer(function(listener){
         var request = 0;
         listener.on('data', function(data){
           request++;

           switch(request){
             case 1 :
                 // fake fetch response!
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
              case 2:
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
              default : throw "WTH!";
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

