var _ = require('underscore');
var strategies = require('./PartitionerStrategies');

var Partitioner = function(strategy){
    this.strategy = strategy || strategies.fixedPartitioner(0);
};

Partitioner.prototype.partition = function(messages){
    var that = this;
    var perMessage = _.map(messages, function(msg) {
        return [that.strategy(msg), msg ];
    });
    var partitionedMessages = _.reduce(perMessage, function(memo, partitionMessage){
        if (!_.has(memo, partitionMessage[0])){
            memo[partitionMessage[0]] = [];
        }
        memo[partitionMessage[0]].push(partitionMessage[1]);
        return memo;
    }, {});
    return partitionedMessages;
}

module.exports = Partitioner;
