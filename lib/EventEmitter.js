
var Consumer = require('./Consumer.js');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

// Trying to closely match `redis` interface
// TODO: should also generate error (need to change Consumer)
var KafkaEventEmitter = function(options) {
    var self = this;
    self.consumer = null;
    self.running = false;
    // EventEmitters inherit a single event listener, see it in action
    self.on('newListener', function(listener) {
      self.running = true;
      if(self.consumer) {
        setTimeout(handler, 0);
        return;
      }

      self.consumer = new Consumer(options);

      var handler = function() {
        self.consumer.consume(function(err, messages){
          for(m in messages) {
            console.log(util.inspect(messages[m].payload.toString()));
            self.emit('message', options.topic, messages[m].payload.toString());
          }
        });
        if(self.running) {
          setTimeout(handler, options.interval)
        } else {
          //TODO: Consumer doesn't have disconnect yet
          //consumer.disconnect();
          //consumer = null;
        }
      }

      self.consumer.connect(function(){
        self.emit('connected');
        setTimeout(handler, 0);
      });
    });

    self.unsubscribe = function() {
      // TODO: multiple topic subscription?
    };

    self.quit = function() {
      // TODO: support for multiple listeners to same EventEmitter?
      self.running = false;
    };
};

util.inherits(KafkaEventEmitter, EventEmitter);

module.exports = KafkaEventEmitter;