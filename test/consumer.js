require('should');
var net = require('net');
var Consumer = require('../lib/consumer').Consumer;


describe("Consumer", function(){
  beforeEach(function(done){
    try {
      this.server.close();
    } catch(ex){}

    this.consumer = new Consumer({ offset : 0 });

    this.server = net.createServer(function(client){

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
      var bytes = new Buffer(24);
      bytes.writeUInt16BE(this.consumer.RequestType.FETCH, 0);
      bytes.writeUInt16BE("test".length, 2);
      bytes.write("test", 4);
      bytes.writeUInt32BE(0, 8);
      bytes.writeUInt32BE(0, 12);
      bytes.writeUInt32BE(0, 16);
      bytes.writeUInt32BE(this.consumer.MAX_SIZE, 20);

      /*
      var bytes = [this.consumer.RequestType.FETCH].pack("n") + 
                  ["test".length].pack("n") + 
                  "test" + 
                  [0].pack("N") + 
                  [0].pack("q").reverse + 
                  [this.consumer.MAX_SIZE].pack("N");
                  */
      this.consumer.encodeRequest(this.consumer.RequestType.FETCH,
                                  "test",
                                  0,
                                  0,
                                  this.consumer.MAX_SIZE).should.eql(bytes);
    });


  });


});

/*
 
 RUBY
  describe "Kafka Consumer" do
  
    it "should encode a request to consume" do
      bytes = [Kafka::RequestType::FETCH].pack("n") + ["test".length].pack("n") + "test" + [0].pack("N") + [0].pack("q").reverse + [Kafka::Consumer::MAX_SIZE].pack("N")
      @consumer.encode_request(Kafka::RequestType::FETCH, "test", 0, 0, Kafka::Consumer::MAX_SIZE).should eql(bytes)
    end

    it "should read the response data" do
      bytes = [0].pack("n") + [1120192889].pack("N") + "ale"
      @mocked_socket.should_receive(:read).and_return([9].pack("N"))
      @mocked_socket.should_receive(:read).with(9).and_return(bytes)
      @consumer.read_data_response.should eql(bytes[2,7])
    end

    it "should send a consumer request" do
      @consumer.stub!(:encoded_request_size).and_return(666)
      @consumer.stub!(:encode_request).and_return("someencodedrequest")
      @consumer.should_receive(:write).with("someencodedrequest").exactly(:once).and_return(true)
      @consumer.should_receive(:write).with(666).exactly(:once).and_return(true)
      @consumer.send_consume_request.should eql(true)
    end

    it "should parse a message set from bytes" do
      bytes = [8].pack("N") + [0].pack("C") + [1120192889].pack("N") + "ale"
      message = @consumer.parse_message_set_from(bytes).first
      message.payload.should eql("ale")
      message.checksum.should eql(1120192889)
      message.magic.should eql(0)
      message.valid?.should eql(true)
    end

    it "should skip an incomplete message at the end of the response" do
      bytes = [8].pack("N") + [0].pack("C") + [1120192889].pack("N") + "ale"
      # incomplete message
      bytes += [8].pack("N")
      messages = @consumer.parse_message_set_from(bytes)
      messages.size.should eql(1)
    end

    it "should skip an incomplete message at the end of the response which has the same length as an empty message" do
      bytes = [8].pack("N") + [0].pack("C") + [1120192889].pack("N") + "ale"
      # incomplete message because payload is missing
      bytes += [8].pack("N") + [0].pack("C") + [1120192889].pack("N")
      messages = @consumer.parse_message_set_from(bytes)
      messages.size.should eql(1)
    end

    it "should read empty messages correctly" do
      # empty message
      bytes = [5].pack("N") + [0].pack("C") + [0].pack("N") + ""
      messages = @consumer.parse_message_set_from(bytes)
      messages.size.should eql(1)
      messages.first.payload.should eql("")
    end

    it "should consume messages" do
      @consumer.should_receive(:send_consume_request).and_return(true)
      @consumer.should_receive(:read_data_response).and_return("")
      @consumer.consume.should eql([])
    end

    it "should loop and execute a block with the consumed messages" do
      @consumer.stub!(:consume).and_return([mock(Kafka::Message)])
      messages = []
      messages.should_receive(:<<).exactly(:once).and_return([])
      @consumer.loop do |message|
        messages << message
        break # we don't wanna loop forever on the test
      end
    end

    it "should loop (every N seconds, configurable on polling attribute), and execute a block with the consumed messages" do
      @consumer = Consumer.new({ :polling => 1 })
      @consumer.stub!(:consume).and_return([mock(Kafka::Message)])
      messages = []
      messages.should_receive(:<<).exactly(:twice).and_return([])
      executed_times = 0
      @consumer.loop do |message|
        messages << message
        executed_times += 1
        break if executed_times >= 2 # we don't wanna loop forever on the test, only 2 seconds
      end

      executed_times.should eql(2)
    end

    it "should fetch initial offset if no offset is given" do
      @consumer = Consumer.new
      @consumer.should_receive(:fetch_latest_offset).exactly(:once).and_return(1000)
      @consumer.should_receive(:send_consume_request).and_return(true)
      @consumer.should_receive(:read_data_response).and_return("")
      @consumer.consume
      @consumer.offset.should eql(1000)
    end

    it "should encode an offset request" do
      bytes = [Kafka::RequestType::OFFSETS].pack("n") + ["test".length].pack("n") + "test" + [0].pack("N") + [-1].pack("q").reverse + [Kafka::Consumer::MAX_OFFSETS].pack("N")
      @consumer.encode_request(Kafka::RequestType::OFFSETS, "test", 0, -1, Kafka::Consumer::MAX_OFFSETS).should eql(bytes)
    end

    it "should parse an offsets response" do
      bytes = [0].pack("n") + [1].pack('N') + [21346].pack('q').reverse
      @mocked_socket.should_receive(:read).and_return([14].pack("N"))
      @mocked_socket.should_receive(:read).and_return(bytes)
      @consumer.read_offsets_response.should eql(21346)
    end
  end
end


*/
