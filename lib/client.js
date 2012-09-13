var BufferMaker = require('buffermaker');

var Client = function(){
};


Client.encodeRequestHeader = function(requestType, topic, partition, requestBodyLength){
  console.log("partition: ", partition);
  var requestHeader = new BufferMaker()
  /* request length = request type + topic length's length + topic length + partition + body length */
  .UInt32BE(2 + 2 + topic.length + 4 + requestBodyLength) 
  .UInt16BE(requestType)
  .UInt16BE(topic.length)
  .string(topic)
  .UInt32BE(partition)
  .make();
  console.log("returning request header: ", requestHeader);
  return requestHeader;
  

};


module.exports = Client;
