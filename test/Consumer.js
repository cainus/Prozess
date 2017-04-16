var util = require('util');
var should = require('should');
var net = require('net');
var bignum = require('bignum');
var _ = require('underscore');
var Consumer = require('../index').Consumer;
var Message = require('../index').Message;
var FetchResponse = require('../index').FetchResponse;
var OffsetsResponse = require('../index').OffsetsResponse;
var BufferMaker = require('buffermaker');
var binary = require('binary');
var sinon = require('sinon');
var HEADER_SIZE = 8;

function bufferFromString(str){
    var bytes = str.split(" ");
    bytes = _.map(bytes, function(datum){
      return parseInt(datum, 16);
    });
    bytes = new Buffer(bytes);
    return bytes;
}

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
    it("creates a consumer with a default topic", function(){
      var consumer = new Consumer();
      consumer.topic.should.equal('test');
    });

    it("creates a consumer with a default partition", function(){
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

    it("creates a consumer with a passed in offset which is cast to bignum", function(){
      var consumer = new Consumer({offset:1111});
      consumer.offset.should.be.an.instanceof(bignum);
      consumer.offset.eq(bignum('1111')).should.equal(true);
    });

    it("creates a consumer with a passed in bignum offset", function(){
      var consumer = new Consumer({offset:bignum('1111')});
      consumer.offset.should.be.an.instanceof(bignum);
      consumer.offset.eq(bignum('1111')).should.equal(true);
    });

    it("creates a consumer with a maximum message size", function(){
      var maxMsgSize = 1234567;
      var consumer = new Consumer({ maxMessageSize: maxMsgSize });
      consumer.maxMessageSize.should.equal(maxMsgSize);
    });

  });

  describe("#sendConsumeRequest", function(){
    it("should send a consumer request", function(done){
      this.timeout(60000);
      var consumer = new Consumer({ port : 9092});
      this.server = net.createServer(function(listener){
        listener.on('data', function(data){
          var expected = "00 00 00 18 00 01 00 04 74 65 73 74 00 00 00 00 00 00 00 00 00 00 00 00 00 10 00 00";
          data.should.eql(bufferFromString(expected));
          var fetchResponse = new FetchResponse(0, [new Message("ale"), new Message("foobar")]);
          listener.write(fetchResponse.toBytes());
        });
      });
      var server = this.server;
      this.server.listen(9092, function(){
        consumer.connect(function(err){
          consumer.offset = bignum(0);
          consumer.sendConsumeRequest(function(err, fetchResponse){
            if (!!err){console.log(err);}
            should.not.exist(err);
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

  describe("#connect", function(done){
    describe("received message is larger than maximum predefined message size", function(){
      var maxMsgSize = new Consumer().MAX_MESSAGE_SIZE;

      afterEach(function() {
        if (net.createConnection.restore) {
          net.createConnection.restore();
        }
      });
      it("throws an error", function(done) {
        var socket = {
          on : function(event, handler) {
            if (event === 'data') {
              handler(new Buffer(maxMsgSize + HEADER_SIZE + 1));
            }
          }
        };
        var createConnection = sinon.stub(net, 'createConnection', function(options, connectionListener) {
          /* simulate async call */
          setTimeout(function() { connectionListener(); }, 10);
          return socket;
        });
        var consumer = new Consumer({topic: "test", port : 1, offset : 1});
        consumer._setRequestMode("fetch");
        consumer.onFetch(function(err) {
          should.exist(err, 'we should emit an error for oversized messages');
          err.message.should.equal(
            'Max message size is ' + maxMsgSize + ' bytes, was exceeded by buffer of size ' +
            (maxMsgSize + HEADER_SIZE + 1) +
            ' Possible causes: bad offset (current offset: ' + 1 + '), corrupt log, message larger than max message size.'
          );
          createConnection.restore();
          done();
        });
        consumer.connect(function(err) {
          should.not.exist(err, 'should not throw an error here: ' + util.inspect(err));
        });
      });
    });

    describe("invalid port", function(){
      it("calls back with an error", function(done){

        var consumer = new Consumer({topic: "test", port : -1});
        consumer.connect(function(err){
          err.message.should.match(/port/i);
          done();
        });

      });
    });
  });


  describe("#getOffsets", function(){

    it("should send an offset request and give a response object" , function(done){
     this.timeout(60000);
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
       consumer.getOffsets(function(err, offsets){
         if (err) { throw err; }
         offsets.length.should.equal(2);
         offsets[1].eq(23).should.equal(true);
         done();
       });
     });

    });

    it("sends offset requests, gather the response OVER TIME, and gives response objects" , function(done){
     this.timeout(60000);
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
       consumer.getOffsets(function(err, offsets){
         if (err) { throw err; }
         offsets.length.should.equal(2);
         offsets[1].eq(23).should.equal(true);
         done();
       });
     });

    });

  });

  describe("#handleFetchData", function(){
    it ("should return a fetchResponse object when possible", function(done){
      var consumer = new Consumer({topic : "test"});
      consumer.setOffset(12);
      consumer._setRequestMode('offsets');
      var randomBytes = new Buffer([0,1,2,3]);
      var messages = [new Message("hello"), new Message("there")];
      var res = new FetchResponse(0, messages).toBytes();
      consumer.responseBuffer = Buffer.concat([res, randomBytes]);
      consumer.handleFetchData(function(err, response){
        should.not.exist(err);
        response.messages[0].payload.should.eql(new Buffer("hello"));
        response.messages[1].payload.should.eql(new Buffer("there"));
        var emptyBuffer = new Buffer([]);
        consumer.responseBuffer.should.eql(emptyBuffer);
        should.not.exist(consumer.requestMode);
        consumer.offset.eq(bignum(12 +
                                  messages[0].toBytes().length +
                                  messages[1].toBytes().length )).should.equal(true);
        done();
      });
    });

  });
  describe("#setOffset", function(){
    it ("can set bignum values as the offset", function(){
      var consumer = new Consumer({topic : "test"});
      consumer.setOffset(bignum(12));
      consumer.offset.eq(bignum(12)).should.equal(true);
    });
    it ("can set non-bignum values as the offset", function(){
      var consumer = new Consumer({topic : "test"});
      consumer.setOffset(12);
      consumer.offset.eq(bignum(12)).should.equal(true);
    });
  });
  describe("#incrementOffset", function(){
    it ("adds integer values to the offset", function(){
      var consumer = new Consumer({topic : "test"});
      consumer.setOffset(12);
      consumer.incrementOffset(14);
      consumer.offset.eq(bignum(26)).should.equal(true);
    });
    it ("adds bignum values to the offset", function(){
      var consumer = new Consumer({topic : "test"});
      consumer.setOffset(12);
      consumer.incrementOffset(bignum(14));
      consumer.offset.eq(bignum(26)).should.equal(true);
    });

  });
  describe("#handleOffsetsData", function(){
    it ("should return an offsetResponse object when possible", function(done){
      var consumer = new Consumer({topic : "test"});
      consumer.setOffset(123);
      consumer._setRequestMode('offsets');
      var randomBytes = new Buffer([0,1,2,3]);
      var res = new OffsetsResponse(0, [456]).toBytes();
      consumer.responseBuffer = Buffer.concat([res, randomBytes]);
      consumer.handleOffsetsData(function(err, offsets){
        offsets[0].eq(bignum(456)).should.equal(true);
        should.not.exist(err);
        consumer.responseBuffer.should.eql(new Buffer([0,1,2,3]));
        should.not.exist(consumer.requestMode);
        done();
      });
    });

  });
  describe("#consume", function(){
    it ("should consume with the latest offset if no offset is provided", function(done){
       this.server = net.createServer(function(listener){
         var requestBuffer = new Buffer([]);

         listener.on('data', function(data){
           requestBuffer = Buffer.concat([requestBuffer, data]);
           data = requestBuffer;
           // check if it's a full request:

           var unpacked = binary(requestBuffer)
                            .word32bu("length")
                            .word16bu("type")
                            .vars;

           var totalExpectedLength = unpacked.length + 4;  // 4 bytes for the length field
           if (data.length >= totalExpectedLength ){
             requestBuffer = requestBuffer.slice(totalExpectedLength);

             switch(unpacked.type){
               case 4 :  // OFFSETS
                   // fake offsets response!
                   var offsetsResponse = new OffsetsResponse(0, [54, 23]);
                   listener.write(offsetsResponse.toBytes());
                   break;


                case 1:  // MESSAGES
                  // fake messages response!

                  var fetchResponse = new FetchResponse(0, [new Message("ale"), new Message("foobar")]);
                  listener.write(fetchResponse.toBytes());
                  break;
                default : throw "unknown request type: " + unpacked.type;
             }
           }
         });

       });

       var consumer = new Consumer({topic : "test"});
       this.server.listen(9092, function(){
         consumer.connect(function(err){
           consumer.consume(function(err, messages){
             should.not.exist(err);
             messages.length.should.equal(2);
             messages[0].payload.toString().should.equal("ale");
             messages[1].payload.toString().should.equal("foobar");
             (consumer.offset.eq(83)).should.equal(true);
             done();
           });
         });
       });
      });
    it("should consume when packets are fragmented", function (done) {
      this.server = net.createServer(function (listener) {
        var requestBuffer = new Buffer([]);

        listener.on('data', function (data) {
          requestBuffer = Buffer.concat([requestBuffer, data]);
          data = requestBuffer;
          // check if it's a full request:

          var unpacked = binary(requestBuffer)
            .word32bu("length")
            .word16bu("type")
            .vars;

          var totalExpectedLength = unpacked.length + 4;  // 4 bytes for the length field
          if (data.length >= totalExpectedLength) {
            requestBuffer = requestBuffer.slice(totalExpectedLength);

            switch (unpacked.type) {
              case 4 :  // OFFSETS
                // fake offsets response!
                var offsetsResponse = new OffsetsResponse(0, [54, 23]);
                listener.write(offsetsResponse.toBytes());
                break;


              case 1:  // MESSAGES
                // fake messages response!

                var fetchResponse = new FetchResponse(0, [new Message("ale"), new Message("foobar")]);
                var respBuf = fetchResponse.toBytes();
                var packet1 = new Buffer(3);
                var packet2 = new Buffer(respBuf.length - 6);
                var packet3 = new Buffer(3);
                respBuf.copy(packet1, 0, 0, 3);
                respBuf.copy(packet2, 0, 3, respBuf.length - 3);
                respBuf.copy(packet3, 0, respBuf.length - 3, respBuf.length);


                listener.write(packet1);
                setTimeout(function () {
                  listener.write(packet2);
                  setTimeout(function () {
                    listener.write(packet3);
                  }, 50);
                }, 50);


                break;
              default :
                throw "unknown request type: " + unpacked.type;
            }
          }
        });

      });

      var consumer = new Consumer({topic: "test"});
      this.server.listen(9092, function () {
        consumer.connect(function (err) {
          consumer.consume(function (err, messages) {
            should.not.exist(err);
            messages.length.should.equal(2);
            messages[0].payload.toString().should.equal("ale");
            messages[1].payload.toString().should.equal("foobar");
            (consumer.offset.eq(83)).should.equal(true);
            done();
          });
        });
      });
    });
  });
});

