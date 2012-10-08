Prozess
=======
[![Build
Status](https://secure.travis-ci.org/cainus/Prozess.png?branch=master)](http://travis-ci.org/cainus/Prozess)

Prozess is a Kafka library for node.js

[Kafka](http://incubator.apache.org/kafka/index.html) is a persistent, efficient, distributed publish/subscribe messaging system.

There are two low-level clients: The Producer and the Consumer:

##Producer example:

```javascript
var Producer = require('Prozess').Producer;

var producer = new Producer('social', {host : 'localhost'});
producer.connect(function(err){
  if (err) {  throw err; }
  console.log("producing for ", producer.topic);
  setInterval(function(){
    var message = { "thisisa" :  "test " + new Date()};
    producer.send(JSON.stringify(message));
  }, 1000);
});
```

##Consumer example:

```javascript
var Consumer = require('Prozess').Consumer;

var options = {host : 'localhost', topic : 'social', partition : 0, offset : 0};
var consumer = new Consumer(options);
consumer.connect(function(){
  console.log("connected!!");
  setInterval(function(){
    console.log("===================================================================");
    console.log(new Date());
    console.log("consuming: " + consumer.topic);
    consumer.consume(function(err, messages){
      console.log(err, messages);
    });
  }, 7000);
});

```

You have to be running Zookeeper and Kafka for this to work, of course.

