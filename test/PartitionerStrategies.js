var should = require('should');
var strategies = require('../lib/PartitionerStrategies');

describe('PartitionerStrategies', function(){

    describe('#fixedPartitioner', function(){
        it("should return a function", function(){
            strategies.fixedPartitioner().should.be.a('function');
        });

        it("function should return the provided value", function(){
            var results = []
            results.push(strategies.fixedPartitioner(0)());
            results.push(strategies.fixedPartitioner(9)());
            results.push(strategies.fixedPartitioner(1)('foo'));
            results.should.eql([0, 9, 1]);
        });
    });

    describe('#randomPartitioner', function(){
        it("should return a function", function(){
            strategies.randomPartitioner().should.be.a('function');
        });

        it("function should return 0 when max partitions is 1", function(){
            var results = [];
            results.push(strategies.randomPartitioner(1)());
            results.push(strategies.randomPartitioner(1)('foo'));
            results.push(strategies.randomPartitioner(1)('bar'));
            results.should.eql([0, 0, 0]);
        });
        it("function should return no value higher as max partitions", function(){
            var maxPartitions = 8;
            var results = [];
            for(var _i=0; _i<25; _i++){
                results.push(strategies.randomPartitioner(maxPartitions)());
            }
            Math.min(results).should.not.be.below(0).and.not.be.above(maxPartitions - 1);
            Math.max(results).should.not.be.below(0).and.not.be.above(maxPartitions - 1);
            Math.min(results).should.not.be.equal(Math.max(results)); // very unlikely with 20+ results
        });

    });

    describe('#hashedPartitioner', function(){
        it("should return a function", function(){
            strategies.hashedPartitioner().should.be.a('function');
        });

        it("function should provide consistent value for given message and given max partitions", function(){
            var msg = { 'payload': 'all your base are belong to us'};
            var results = []
            results.push(strategies.hashedPartitioner(9)(msg));
            results.push(strategies.hashedPartitioner(9)(msg));
            results.push(strategies.hashedPartitioner(9)(msg));
            results.push(strategies.hashedPartitioner(19)(msg));
            results.push(strategies.hashedPartitioner(19)(msg));
            results.should.eql([7, 7, 7, 4, 4]);
        });

        it("function should provide different value for different messages", function(){
            var results = []
            results.push(strategies.hashedPartitioner(10000)({ 'payload': 'all your base are belong to us'}));
            results.push(strategies.hashedPartitioner(10000)({ 'payload': 'all your foo are belong to us'}));
            results.should.eql([3336, 8339]);
        });

    });


});