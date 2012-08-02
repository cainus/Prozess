var net = require('net');
var _ = require('underscore');
var Message = require('./message').Message;

var Consumer = function(options){
  this.MAX_SIZE = 1024 * 1024; // 1 megabyte
  this.DEFAULT_POLLING_INTERVAL = 2; // 2 seconds
  this.MAX_OFFSETS = 1;
  this.LATEST_OFFSET = -1;
  this.EARLIEST_OFFSET = -2;
  this.RequestType = {
    PRODUCE      : 0,
    FETCH        : 1,
    MULTIFETCH   : 2,
    MULTIPRODUCE : 3,
    OFFSETS      : 4
  };

  this.topic = options.topic || 'test';
  this.partition = options.partition || 0;
  this.host = options.host || 'localhost';
  this.port = options.port || 9092;
  this.offset = options.offset;
  this.max_size = options.max_size || this.MAX_SIZE;
  this.polling = options.polling  || this.DEFAULT_POLLING_INTERVAL;
  this.request_type = null;
  
};


Consumer.prototype.connect = function(){
  this.client = net.createConnection(this.port, this.host);
};


Consumer.prototype.encodedRequestSize = function(){
  var size = 2 + 2 + this.topic.length + 4 + 8 + 4;
  //[size].pack("N")
  console.log("size: ", size);
  var buffer = new Buffer(4);
  buffer.writeUInt32BE(size, 0);
  return buffer;

};


Consumer.prototype.encodeRequest =
    function( request_type, topic, partition, offset, max_size){

      return new BufferMaker()
              .UInt16BE(request_type)
              .UInt16BE(topic.length)
              .string(topic)
              .UInt32BE(0)
              .UInt32BE(0)
              .UInt32BE(0)
              .UInt32BE(max_size)
              .make();
      //offset       = [offset].pack("q").reverse # DIY 64bit big endian integer
};

exports.Consumer = Consumer;


var BufferMaker = function(){
  this.plan = [];
};

BufferMaker.prototype.UInt16BE = function(val){
  this.plan.push({ type : "UInt16BE", value : val});
  return this;
};

BufferMaker.prototype.UInt32BE = function(val){
  this.plan.push({ type : "UInt32BE", value : val});
  return this;
};

BufferMaker.prototype.string = function(val){
  this.plan.push({ type : "string", value : val});
  return this;
};

BufferMaker.prototype.make = function(){
  var bytecount = 0;
  var offset = 0;
  _.each(this.plan, function(item){
    switch(item.type){
      case "UInt16BE": bytecount += 2; break;
      case "UInt32BE": bytecount += 4; break;
      case "string": bytecount += item.value.length; break;
    }
  });
  var buffer = new Buffer(bytecount);
  _.each(this.plan, function(item){
    switch(item.type){
      case "UInt16BE": buffer.writeUInt16BE(item.value, offset); offset += 2; break;
      case "UInt32BE": buffer.writeUInt32BE(item.value, offset); offset += 4; break;
      case "string": buffer.write(item.value, offset); offset += item.value.length; break;
    }
  });
  return buffer;

};


/**RUBY
module Kafka
  class Consumer

    include Kafka::IO

    MAX_SIZE = 1024 * 1024        # 1 megabyte
    DEFAULT_POLLING_INTERVAL = 2  # 2 seconds
    MAX_OFFSETS = 1
    LATEST_OFFSET = -1
    EARLIEST_OFFSET = -2

    attr_accessor :topic, :partition, :offset, :max_size, :request_type, :polling

    def initialize(options = {})
      self.topic        = options[:topic]     || "test"
      self.partition    = options[:partition] || 0
      self.host         = options[:host]      || "localhost"
      self.port         = options[:port]      || 9092
      self.offset       = options[:offset]
      self.max_size     = options[:max_size]  || MAX_SIZE
      self.polling      = options[:polling]   || DEFAULT_POLLING_INTERVAL
      connect(host, port)
    end

    def loop(&block)
      messages = []
      while (true) do
        messages = consume
        block.call(messages) if messages && !messages.empty?
        sleep(polling)
      end
    end

    def consume
      self.offset ||= fetch_latest_offset
      send_consume_request
      data = read_data_response
      parse_message_set_from(data)
    rescue SocketError
      nil
    end

    def fetch_latest_offset
      send_offsets_request
      read_offsets_response
    end

    def send_offsets_request
      write(encoded_request_size)
      write(encode_request(Kafka::RequestType::OFFSETS, topic, partition, LATEST_OFFSET, MAX_OFFSETS))
    end

    def read_offsets_response
      read_data_response[4,8].reverse.unpack('q')[0]
    end

    def send_consume_request
      write(encoded_request_size)
      write(encode_request(Kafka::RequestType::FETCH, topic, partition, offset, max_size))
    end

    def read_data_response
      data_length = read(4).unpack("N").shift
      data = read(data_length)
      # TODO: inspect error code instead of skipping it
      data[2, data.length]
    end

    def encoded_request_size
      size = 2 + 2 + topic.length + 4 + 8 + 4
      [size].pack("N")
    end

    def encode_request(request_type, topic, partition, offset, max_size)
      request_type = [request_type].pack("n")
      topic        = [topic.length].pack('n') + topic
      partition    = [partition].pack("N")
      offset       = [offset].pack("q").reverse # DIY 64bit big endian integer
      max_size     = [max_size].pack("N")
      request_type + topic + partition + offset + max_size
    end

    def parse_message_set_from(data)
      messages = []
      processed = 0
      length = data.length - 4
      while (processed <= length) do
        message_size = data[processed, 4].unpack("N").shift + 4
        message_data = data[processed, message_size]
        break unless message_data.size == message_size
        messages << Kafka::Message.parse_from(message_data)
        processed += message_size
      end
      self.offset += processed
      messages
    end

  end
end
*/
