var Producer = require('./lib/Producer');

var producer = new Producer('social', {host : 'localhost'});
producer.connect(function(err){
  if (err) {  throw err; }
  console.log("producing for ", producer.topic);
  producer.send('{"thisisa" : "test' + new Date() + '"}');
});
