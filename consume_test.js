var Consumer = require('./lib/Consumer');

var options = {host : 'xtnvkafka01.xt.local', topic : 'social', partition : 0};
options = {host : 'localhost', topic : 'social', partition : 0};
var consumer = new Consumer(options);
consumer.connect(function(){
  console.log("connected!!");
  setInterval(function(){
    console.log("gonna consume!!");
    consumer.consume(function(err, messages){
      console.log(err, messages);
    });
  }, 2000);
});


