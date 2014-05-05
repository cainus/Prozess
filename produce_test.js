var Producer = require('./Producer');

var producer = new Producer('social', {host : 'localhost'});
producer.connect();
producer.on('error', function(err){
  console.log("error: ", err);
});

console.log("producing for ", producer.topic);
setInterval(function(){
console.log("sending...");
  producer.send('{"thisisa" : "test' + new Date() + '"}', function(err){
    
    if(typeof(err) != "undefined")
      console.log("send error: ", err);
  
  });
}, 1000);
