var Producer = require('./lib/Producer');

var producer = new Producer({topic : 'social', host : 'localhost'});
producer.connect(function(err){
  if (err) {  throw err; }
  producer.send({"thisisa" : "test" + new Date()});
});
