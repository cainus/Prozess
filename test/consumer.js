require('should');
var net = require('net');
var _ = require('underscore');
var Consumer = require('../lib/consumer').Consumer;
var BufferMaker = require('buffermaker');

describe("Consumer", function(){
  beforeEach(function(done){
    try {
      this.server.close();
    } catch(ex){}

    this.consumer = new Consumer({ offset : 0 });

    this.server = net.createServer(function(client){
      client.on('data', function(data){
        console.log("dater: ", data);
      });

    });

    this.server.listen(9092, function(){
      done();
    });

  });

  beforeEach(function(){
    try {
      this.server.close();
    } catch(ex){}
  });

  describe("Kafka Consumer", function(){

    it("should have a Kafka::RequestType::FETCH", function(){
      this.consumer.RequestType.FETCH.should.equal(1);
    });

    it("should have a default topic", function(){
      this.consumer.topic.should.equal('test');
    });

    it("should have a default partition", function(){
      this.consumer.partition.should.equal(0);
    });

    it("should have a default polling time", function(){
      this.consumer.polling.should.equal(2);
    });

    it("should set a topic on initialize", function(){
      this.consumer = new Consumer({topic:"newtopic"});
      this.consumer.topic.should.equal("newtopic");
    });
 
    it("should set a partition on initialize", function(){
      this.consumer = new Consumer({partition:3});
      this.consumer.partition.should.equal(3);
    });

    it("should set default host if none is specified", function(){
      this.consumer.host.should.equal('localhost');
    });

    it("should set a default port if none is specified", function(){
      this.consumer.port.should.equal(9092);
    });
   
    it("should not have a default offset but be able to set it", function(){
      var x = this.consumer.offset.should.not.be.ok;
      this.consumer = new Consumer({offset:1111});
      this.consumer.offset.should.equal(1111);
    });

    it("should have a max size", function(){
      this.consumer.max_size.should.equal(1048576);
    });

    it("should return the size of the request", function(){
      this.consumer.topic = "someothertopicname";
      console.log("someothertopic".length);
      console.log(this.consumer.encodedRequestSize());
      var buffer = new Buffer(4);
      buffer.writeUInt32BE(38, 0);
      this.consumer.encodedRequestSize().should.eql(buffer);
    });

    

    it("should encode a request to consume", function(){
      var bytes = new BufferMaker()
      .UInt16BE(this.consumer.RequestType.FETCH)
      .UInt16BE(this.consumer.topic.length)
      .string("test")
      .UInt32BE(0)
      .UInt32BE(0)
      .UInt32BE(0)
      .UInt32BE(this.consumer.MAX_SIZE)
      .make();


      //var bytes = new Buffer(24);

      this.consumer.encodeRequest(this.consumer.RequestType.FETCH,
                                  "test",
                                  0,
                                  0,
                                  this.consumer.MAX_SIZE).should.eql(bytes);
    });

    it("should send a consumer request", function(done){
     this.server = net.createServer(function(listener){
       listener.on('data', function(data){
         var expected = "00 00 00 18 00 01 00 04 74 65 73 74 00 00 00 00 00 00 00 00 00 00 00 00 00 10 00 00";
         expected = expected.split(" ");
         expected = _.map(expected, function(datum){
           return parseInt(datum, 16);
         });
         data.should.eql(new Buffer(expected));
         done();
       });

     });

     this.server.listen(9092, function(){
     });
     this.consumer.connect();
     this.consumer.sendConsumeRequest();
    });
describe("encodeOffsetsRequests", function(){
  it("should fetch the latest offset" , function(){
    var expectedBytes = new BufferMaker();
    
    var consumer = new Consumer();
    var request = consumer.encodeOffsetsRequest(-1);
    request.should.eql(expectedBytes);
  });
});
    describe("getLatestOffsets", function(){
      it("should retrive latest max offset", function(){
        var consumer = new Consumer();
        //consumer.getLatestOffsets( ...
      });
    });


  });


});

