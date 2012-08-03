var net = require('net');
var _ = require('underscore');
var Message = require('./message').Message;
var BufferMaker = require('./buffermaker').BufferMaker;
var binary = require('binary');

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
  this.unprocessedData = new Buffer([]);
};

Consumer.prototype.addData = function(data){
  console.log("want to add: ", data);
  var that = this;
  console.log("unprocessData", that.unprocessedData);
  that.unprocessedData = Buffer.concat([that.unprocessedData, data]);
                                  // add data to unprocessed buffer
  console.log("total unprocessedData: ", that.unprocessedData);
  that.offset += data.length;  // increment the offset, because we got new data
  console.log("offset is now: ", that.offset);
};

Consumer.prototype._handleData = function(data){
    var that = this;
    console.log("==============================");
    console.log("offset is now: ", that.offset);
    console.log("data: ", data);
    console.log("unprocessData", that.unprocessedData);
    var currentData = Buffer.concat([that.unprocessedData, data]);

    // if the length byte of unprocessed matches the length of unprocessed,
    // we have a full message!  (otherwise exit);

    // if we don't have 4 bytes, we don't know the length, so exit.
    if (currentData < 4){
      console.log("LESS THAN 4 ADD");
      that.addData(data);
      return;  // wait for more data
    }

    // get the length of the message, so we know what length to expect
    var vars = binary.parse(currentData)
        .word32bu('length')
        .vars;
    var expectedLength = vars.length;

    // if we don't have enough data to match that length, exit
    console.log("expecting: ", expectedLength);
    console.log("have: ", currentData.length);
    if (currentData.length < expectedLength){
      console.log("LESS THAN EXPECTED ADD");
      that.addData(data);
      return;  // wait for more data
    }

    // we have a complete message (and maybe more) at this point!

    // extract the data up until the expectedLength and create a message
    // from it
    console.log("data: ", currentData);

    try {
      vars = binary.parse(currentData)
          .word32bu('length')
          .word16bs('error')
          .vars;
      console.log("STATUS: ", vars.error);
      if (vars.error !== 0){
        console.log("ERROR: ", vars.error);
        return;
      }

      vars = binary.parse(currentData)
          .word32bu('length')
          .word16bs('error')
          .buffer('message', expectedLength)
          .vars;
    } catch(ex){
      console.log("ex: ", ex);
    }

    var message = Message.parseFrom(vars.message);
    console.log("message: ", message);

    // remove that data from unprocessedData
    console.log("removing message from ", that.unprocessedData);
    that.unprocessedData = that.unprocessedData.slice((expectedLength - data.length) + 4);
    console.log("unprocessed is now  ", that.unprocessedData);
    
    that.onMessage(message);

    that.offset += data.length;  // increment the offset, because we got new data

};


Consumer.prototype.connect = function(){
  var that = this;
  this.socket = net.createConnection(this.port, this.host);
  this.socket.on('connect', function(){
    console.log("connected");
  });

  this.socket.on('data', function(data){
    
    that._handleData(data);

  });



  this.socket.on('end', function(){
    console.log("end");
  });
  this.socket.on('timeout', function(){
    console.log("timeout");
  });
  this.socket.on('drain', function(){
    console.log("connected");
  });
  this.socket.on('error', function(err){
    console.log("error: ", err);
  });
  this.socket.on('close', function(){
    console.log("close");
  });
};

Consumer.prototype.encodedRequestSize = function(){
  var size = 2 + 2 + this.topic.length + 4 + 8 + 4;
  //[size].pack("N")
  console.log("size: ", size);
  var buffer = new Buffer(4);
  buffer.writeUInt32BE(size, 0);
  return buffer;

};


Consumer.prototype.onMessage = function(message){
  console.dir(message);
};



Consumer.prototype.readDataResponse = function(data){
  // data is probably not a single message.
  
  var dataLength = read(4).unpack("N").shift;
  data = read(dataLength);
  // TODO: inspect error code instead of skipping it
  return data.slice(2, data.length);
};


Consumer.prototype.encodeRequest =
    function( request_type, topic, partition, offset, max_size){

      return new BufferMaker()
              .UInt16BE(request_type)
              .UInt16BE(topic.length)
              .string(topic)
              .UInt32BE(partition)
              .UInt64BE(offset)
              .UInt32BE(max_size)
              .make();
      //offset       = [offset].pack("q").reverse # DIY 64bit big endian integer
};

Consumer.prototype.sendConsumeRequest = function(){
  this.socket.write( this.encodedRequestSize() );
  this.socket.write( this.encodeRequest( this.RequestType.FETCH,
                                         this.topic,
                                         this.partition,
                                         this.offset,
                                         this.max_size));
};

exports.Consumer = Consumer;



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
