require('should');
var net = require('net');
var Producer = require('../lib/producer').Producer;


describe("Producer", function(){
  beforeEach(function(){
    this.producer = new Producer();
  });

  describe("Kafka Producer", function(){


    it("should have a request type id", function(){
      this.producer.requestType.should.equal(0);
    });

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


  });
});
