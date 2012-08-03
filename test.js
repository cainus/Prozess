var Consumer = require('./lib/consumer').Consumer;


var consumer = new Consumer({offset : 0});
consumer.connect();


setInterval(function(){
  console.log("gonna consume!!");
  consumer.sendConsumeRequest();
}, 15000);
