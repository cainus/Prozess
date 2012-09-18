var Consumer = require('./lib/consumer');


var consumer = new Consumer({host : 'xtnvkafka01.xt.local', topic : 'social', partition : 0});
consumer.connect(function(){
  console.log("connected!!");
  setInterval(function(){
    console.log("gonna consume!!");
    consumer.consume(function(err, messages){
      console.log(err, messages);
    });
  }, 2000);
});


