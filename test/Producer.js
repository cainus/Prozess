var should = require('should');
var Producer = require('../index').Producer;
var BufferMaker = require('buffermaker');
var net = require('net');

describe("Producer", function(){
  beforeEach(function(){
      this.producer = new Producer('test');
  });

  describe("Kafka Producer", function(){
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

    it("should error if topic is not supplied", function(){
      try {
        new Producer();
        should.fail("expected exception was not raised");
      } catch(ex){
        ex.should.equal("the first parameter, topic, is mandatory.");
      }
    });
    it("should throw an error if no Kafka server is present", function(done) {
      this.producer.port = 8542;
      this.producer.on('error', function(err) {
        should.exist(err);
        done();
      });
      this.producer.connect(function() { 
        should.fail("should not get here"); 
      });
    });
    /*
    describe("sending messages", function() {
      it("SHOULD coerce a single message into a list", function(done) { 
        var that = this;
        var server = net.createServer(function (socket) {
          server.on('data', function(data){
            console.log("DATA: ", data, " | ", data.toString(), data.length);
            done();
          });
          server.listen(8542, function() { //'listening' listener
            console.log('server bound');
            server.timeout(300000);
            that.producer.port = 8542;
            that.producer.on('error', function() {
              console.log('straw');
            });
            that.producer.connect(function() {
              console.log("connected");
              that.producer.send(new Message('foo'));
              console.log("post-send");
            });
          });
        });
      });
    }); */
  });
});
