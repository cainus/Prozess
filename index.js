var dir = './lib/';
if (process.env.PROZESS_COVERAGE){
  var dir = './lib-cov/';
}
exports.Producer = require(dir + 'Producer');
exports.Consumer = require(dir + 'Consumer');
exports.Message = require(dir + 'Message');
exports.FetchRequest = require(dir + 'FetchRequest');
exports.ProduceRequest = require(dir + 'ProduceRequest');
exports.Request = require(dir + 'Request');
exports.Response = require(dir + 'Response');
exports.FetchResponse = require(dir + 'FetchResponse');
exports.OffsetsResponse = require(dir + 'OffsetsResponse');
exports.OffsetsRequest = require(dir + 'OffsetsRequest');
exports.EventEmitter = require(dir + 'EventEmitter');
