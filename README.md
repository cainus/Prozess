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

##Installation:

     npm install prozess

##Checkout the code and run the tests:

     $ git clone https://github.com/cainus/Prozess.git
     $ cd Prozess ; make test-cov && open coverage.html


##Kafka Compatability matrix:

<table>
  <tr>
    <td>Kafka 0.7.2 Release</td><td>Supported</td>
  <tr>
    <td>Kafka 0.7.1 Release</td><td>Supported</td>
  <tr>
    <td>Kafka 0.7.0 Release</td><td>Supported</td>
  <tr>
    <td>kafka-0.6</td><td>Consumer-only support.</td>
  <tr>
    <td>kafka-0.05</td><td>Not Supported</td>
</table>

Versions taken from http://incubator.apache.org/kafka/downloads.html