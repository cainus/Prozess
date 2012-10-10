var should = require('should');
var Producer = require('../index').Producer;
var Message = require('../index').Message;
var ProduceRequest = require('../index').ProduceRequest;
var BufferMaker = require('buffermaker');
var binary = require('binary');
var net = require('net');

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

describe("Producer", function(){
  beforeEach(function(done){
    this.producer = new Producer('test');
    if (!this.server) {
      this.server = null;
    }
    closeServer(this.server, done);
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
    describe("sending messages", function() {
      it("SHOULD coerce a single message into a list", function(done) { 
        var that = this;
        this.server = net.createServer(function (socket) {
              socket.on('data', function(data){
            if (false)
                   console.log("DATA: ", data, " | ", data.toString(), data.length);
            var unpacked = binary.parse(data)
            .word32bu('length')
            .word16bs('error')
            .tap( function(vars) {
              this.buffer('body', vars.length);
            })
            .vars;
            var request = ProduceRequest.fromBytes(data);
            request.messages.length.should.equal(1);
            request.messages[0].payload.toString().should.equal("foo");
            done();
          });
        });
        this.server.listen(8542, function() {
          that.producer.port = 8542;
          that.producer.on('error', function() {
          });
          that.producer.connect(function() {
            var message = new Message('foo');
            that.producer.send(message);
          });
        });
      });
    });
  });
});
