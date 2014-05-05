Prozess
=======
[![Build
Status](https://secure.travis-ci.org/cainus/Prozess.png?branch=master)](http://travis-ci.org/cainus/Prozess)
[![Coverage Status](https://coveralls.io/repos/cainus/Prozess/badge.png?branch=master)](https://coveralls.io/r/cainus/Prozess)

Prozess is a Kafka library for node.js

[Kafka](http://incubator.apache.org/kafka/index.html) is a persistent, efficient, distributed publish/subscribe messaging system.

There are two low-level clients: The Producer and the Consumer:

##Producer example:

```javascript
var Producer = require('Prozess').Producer;

var producer = new Producer('social', {host : 'localhost'});
producer.connect();
console.log("producing for ", producer.topic);
producer.on('error', function(err){
  console.log("some general error occurred: ", err);  
});
producer.on('brokerReconnectError', function(err){
  console.log("could not reconnect: ", err);  
  console.log("will retry on next send()");  
});

setInterval(function(){
  var message = { "thisisa" :  "test " + new Date()};
  producer.send(JSON.stringify(message), function(err){
    if (err){
      console.log("send error: ", err);
    } else {
      console.log("message sent");
    }
  });
}, 1000);
```

##Consumer example:

```javascript
var Consumer = require('Prozess').Consumer;

var options = {host : 'localhost', topic : 'social', partition : 0, offset : 0};
var consumer = new Consumer(options);
consumer.connect(function(err){
  if (err) {  throw err; }
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

A `Consumer` can be constructed with the following options (default values as
shown below):

```javascript
var options = {
  topic: 'test',
  partition: 0,
  host: 'localhost',
  port: 9092,
  offset: null, // Number, String or BigNum
  maxMessageSize: Consumer.MAX_MESSAGE_SIZE,
  polling: Consumer.DEFAULT_POLLING_INTERVAL
};
```

##Documentation

### `var producer = new Producer(options)`

```ocaml
type Message := String | Buffer

Producer := ({
  topic: String,
  partition?: Number,
  host?: String,
  port?: Number,
  connectionCache?: Boolean
}) => EventEmitter & {
  connect: () => void,
  send: (Array<Message> | Message, opts?: {
    partition?: Number,
    topic?: String
  }, cb: Callback<>) => void
}
```

To create a `producer` you must call `Producer()` with various
  options. The only required argument is the topic your
  producing to.
  
```js
var Producer = require('prozess').Producer

var producer = new Producer({ topic: 'foos' })
```

#### `options.topic`

`options.topic` must be a `String` and is the topic
  in kafka that you will producer to
  
#### `options.partition`

`options.partition` is an optional `Number` and 
  defaults to `0`. You can specify this if you want
  to change which `partition` you publish to.
  
#### `options.host` and `options.port`

`options.host` determines the host location of the
  kafka node you are connecting to and `options.port`
  determines the port.
  
These default to `"localhost"` and `9092` which are 
  the default kafka ports.
  
#### `options.connectionCache`

`options.connectionCache` is a `Boolean` you can set
  to opt in into connection caching. By default `prozess`
  will create one TCP connection per topic.
  
If you set `options.connectionCache` to true then 
  `prozess` will use one TCP connection per host & port.
  
#### `producer.connect(Callback)`

You can call `producer.connect(cb)` to open your connection
  to kafka, the `cb` you pass in will be called once the
  connection is open.
  
You must call `.connect()` before calling `.send()`

#### `producer.send(Message, opts?: Object, Callback)`

To produce messages to kafka you should call `.send()` with
  either an array of `String`'s or a single `String`.
  
You can pass `.send()` an optional options argument to 
  customize the `topic` & `partition` for this single `send()`
  request.
  
You must also supply a `Callback` to handle any asynchronous
  errors.

##Installation:

     npm install prozess

##Checkout the code and run the tests:

     $ git clone https://github.com/cainus/Prozess.git
     $ cd Prozess ; make test-cov && open coverage.html


##Kafka Compatability matrix:

<table>
  <tr>
     <td>Kakfa 0.8.0 Release</td><td>Not Supported</td>
  </tr>
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
