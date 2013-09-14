var crypto = require('crypto');

exports.fixedPartitioner = function(fixedPartition){
    fixedPartition = fixedPartition || 0;
    return function(message){
        return fixedPartition;
    }
};

exports.randomPartitioner = function(maxPartitions){
    return function(message){
        return Math.floor(Math.random() * maxPartitions);
    }
};

exports.hashedPartitioner = function(maxPartitions, algorithm, toHash){
    algorithm = algorithm || 'md5';
    toHash = toHash || function(msg){ return msg.payload; };
    return function(message){
        var num = parseInt(crypto.createHash(algorithm).update(toHash (message)).digest('hex').substr(0, 8), 16);
        return num % maxPartitions;
    };
};
