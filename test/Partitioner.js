var should = require('should');
var Partitioner = require('../lib/Partitioner');

describe('Partitioner', function(){

    it("should provide message mapped to default partition", function(){
        var partitioner = new Partitioner();
        partitioner.partition(['foo']).should.eql({0: ['foo']});
    });

    it("should provide multiple messages mapped to one partition", function(){
        var partitioner = new Partitioner();
        partitioner.partition(['foo', 'bar']).should.eql({0: ['foo', 'bar']});
    });

    it("should provide multiple messages mapped per partition", function(){
        var partitions = [0,3,5,3];
        var strategy = function() { return partitions.shift(); };

        var partitioner = new Partitioner(strategy);
        partitioner.partition(['foo', 'bar', 'foo-bar', 'foo2000'])
            .should.eql({0: ['foo'], 3: ['bar', 'foo2000'], 5: ['foo-bar']});
    });

})