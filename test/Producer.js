var should = require('should');
var Producer = require('../index').Producer;
var Message = require('../index').Message;
var ProduceRequest = require('../index').ProduceRequest;
var BufferMaker = require('buffermaker');
var binary = require('binary');
var net = require('net');
var EventEmitter = require('events').EventEmitter;
var sinon = require('../lib/sinonPatch');
var Partitioner = require('../lib/Partitioner');

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
  afterEach(function(){
    sinon.restoreAll();
  });

  describe("Kafka Producer", function(){
    it("should have a default topic id", function(){
      this.producer.topic.should.equal('test');
    }); 

    it("should have a default partitioner", function(){
      this.producer.partitioner.should.be.a('object');
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
    describe("no server is present", function(){
      it("emits an error", function(done) {
        this.producer.port = 8542;
        this.producer.on('error', function(err) {
          should.exist(err);
          done();
        });
        this.producer.connect(function(err) {
        });
      });
    });
    describe("#connect", function() {
      it("sets keep-alive on underlying socket on connect", function(done) { 
        /* decorate underlying Socket function to set a property */
        var isSetKeepAliveSet = false;
        var setKeepAlive = net.Socket.prototype.setKeepAlive;
        net.Socket.prototype.setKeepAlive = function(setting, msecs) {
          isSetKeepAliveSet = true;
          setKeepAlive(setting, msecs);
        };
        this.server = net.createServer(function(connection) {
        });
        this.server.listen(9998);
        this.producer.port = 9998;
        this.producer.on('connect', function(){
          done();
        });
        this.producer.connect();
        isSetKeepAliveSet.should.equal(true);
        net.Socket.prototype.setKeepAlive = setKeepAlive;
      });
    });
    describe("#send", function() {
      it("should attempt a reconnect on send if disconnected", function(done) {
        var connectionCount = 0;
        sinon.stub(net, "createConnection", function(port, host){
          host.should.equal("localhost");
          port.should.equal(8544);
          var fakeConn = new EventEmitter();
          fakeConn.setKeepAlive = function(somebool, interval){
            somebool.should.equal(true);
            interval.should.equal(1000);
          };
          fakeConn.write = function(data, cb){
            if (connectionCount === 0){
              connectionCount = connectionCount + 1;
              cb(new Error('This socket is closed.'));
            } else {
              connectionCount = connectionCount + 1;
              cb();
            }
          };
          setTimeout(function(){
            fakeConn.emit('connect');
          }, 1);
          return fakeConn;
        });
        var that = this;
        this.producer = new Producer('test');
        this.producer.port = 8544;

        this.producer.once('connect', function(){
          that.producer.send('foo', function(err) {
            should.not.exist(err);
            connectionCount.should.equal(2);
            done();
          });
        });
        this.producer.connect();
      });
    it("should coerce a non-Message object into a Message object before sending", function(done) { 
      var that = this;
      this.server = net.createServer(function (socket) {
            socket.on('data', function(data){
          var unpacked = binary.parse(data)
          .word32bu('length')
          .word16bs('error')
          .tap( function(vars) {
            this.buffer('body', vars.length);
          })
          .vars;
          var request = ProduceRequest.fromBytes(data);
          request.messages.length.should.equal(1);
          request.messages[0].payload.toString().should.equal('this is not a message');
          done();
        });
      });
      this.server.listen(8542, function() {
        that.producer.port = 8542;
        that.producer.on('error', function() {
        });
        that.producer.on('connect', function(){
          var messages = ['this is not a message'];
          that.producer.send(messages, function(err){
            should.not.exist(err);
          });
        });
        that.producer.connect();
      });
    });
    it("if there's an error, report it in the callback", function(done) { 
      var that = this;
      this.server = net.createServer(function(socket) {});
      this.server.listen(8542, function() {
        that.producer.port = 8542;
        that.producer.on('connect', function(){
          that.producer.connection.write = function(bytes, cb) {
            should.exist(cb);
            cb('some error');
          };
          var message = new Message('foo');
          that.producer.send(message, function(err) {
            err.toString().should.equal('some error');
            done();
          });
        });
        that.producer.connect();
      });
    });
    it("should coerce a single message into a list", function(done) { 
      var that = this;
      this.server = net.createServer(function (socket) {
            socket.on('data', function(data){
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
        });
      });
      this.server.listen(8542, function() {
        that.producer.port = 8542;
        that.producer.on('error', function(err) {
          should.fail('should not get here');
        });
        that.producer.on('connect', function(){
          var message = new Message('foo');
          that.producer.send(message, function(err) {
            should.not.exist(err);
            done();
          });
        });
        that.producer.connect();
      });
    });
    it("handle non-ascii utf-8", function(done) { 
      var that = this;
      var testString = "fo\u00a0o";
      this.server = net.createServer(function (socket) {
            socket.on('data', function(data){
          var unpacked = binary.parse(data)
          .word32bu('length')
          .word16bs('error')
          .tap( function(vars) {
            this.buffer('body', vars.length);
          })
          .vars;
          var request = ProduceRequest.fromBytes(data);
          request.messages.length.should.equal(1);
          request.messages[0].payload.toString().should.equal(testString);
        });
      });
      this.server.listen(8542, function() {
        that.producer.port = 8542;
        that.producer.on('error', function(err) {
          should.fail('should not get here');
        });
        that.producer.on('connect', function(){
          var message = new Message(testString);
          that.producer.send(message, function(err) {
            should.not.exist(err);
            done();
          });
        });
        that.producer.connect();
      });
    });
  });
});
});
