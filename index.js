//==============================
//SETUP
//==============================

// Libraries
var fs = require('fs'),
    AWS = require('aws-sdk'),
    async = require('async');

//==============================
//CLASSES
//==============================

/* The class to be exported
** Params:
**     config: table containing accessKeyId, secretAccessKey, region (table)
**     streamArn: the ARN for the stream wanting to be read (string)
**     previousShards: table containing shards and their sequence numbers; key: shardId, val: sequenceNumber (table)
*/
//::::::::::::::::::::::::::::::
module.exports = DynamoDBStream

function DynamoDBStream(tableName, config, streamArn, previousShards) {

    // Set AWS Config
    var creds = new AWS.Credentials(config.accessKeyId, config.secretAccessKey);

    // Initialise DynamoDB Object and DynamoDB Stream Object
    this._tableName = tableName;
    this._dynamodbstreams = new AWS.DynamoDBStreams({"credentials": creds, "region": config.region});

    // Attributes
    this._streamArn = streamArn;
    this._shardSequenceNumbers = (typeof previousShards == "object") ? previousShards : {};

    // event handlers
    this._onRecord = null;      // To be called whenever a record is received
    this._onShardClosed = null; // To be called whenever a shard has been drained
    this._onFinished = null;    // To be called at the very end

    // default limit, run forever
    this._endTime = null;

}
//::::::::::::::::::::::::::::::


//==============================
// PUBLIC METHODS
//==============================


//::::::::::::::::::::::::::::::
// Function that sets the "_onRecord" callback
DynamoDBStream.prototype.onRecord = function(callback) {
    this._onRecord = callback;
}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// Function that sets the "_onRecord" callback
DynamoDBStream.prototype.onShardClosed = function(callback) {
    this._onShardClosed = callback;
}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// The main function which executes the other functions
DynamoDBStream.prototype.Run = function(limit_seconds, callback) {

    // Set end time for loop with a 10% wiggle
    if (typeof limit_seconds == "function") {
        callback = limit_seconds;
        limit_seconds = undefined;
    } 
    this._onFinished = callback;

    if (limit_seconds) {
        var wiggle = limit_seconds * 0.1 * Math.random();
        var timeLimit = Math.round((limit_seconds - wiggle) * 1000);
        this._endTime = new Date().getTime() + timeLimit;
    }

    //..............................
    // Get the shards for the current table
    this._getShards(function(err, shards) {

        //..............................
        // Loop over each of the shards in parallel looking for results
        async.each(shards, 

            //..............................
            // Function that iterates over each shard
            function _shardIterator(shard, next) {

                // Ignore closed shards
                if (shard.ShardId in this._shardSequenceNumbers && this._shardSequenceNumbers[shard.ShardId] == "closed") {
                    // console.log("Skipping closed shard: " + shard.ShardId)
                    return next();
                } else {
                    // console.log("Processing shard: " + shard.ShardId)
                }

                this._getIterator(shard, function(err, iterator) {

                    if (err) return next(err);
                    async.doWhilst(

                        //..............................
                        function _eachIterator(callback) {

                            this._getRecords(iterator, function(err, records, nextIterator) {

                                iterator = nextIterator;

                                async.eachSeries(records, 

                                    //..............................
                                    // Function that iterates over each record
                                    function _recordEvent(record, next) {

                                        this._onRecord(this._tableName, shard, record, next);

                                    }.bind(this),

                                    //..............................
                                    // Callback that runs after last iteration of "_recordEvent"
                                    function _recordCallback(err) {

                                        callback(err);

                                    }.bind(this)
                                )

                            }.bind(this));
                        }.bind(this),

                        //..............................
                        function _eachIteratorTest() {
                            if (this._endTime == null || new Date().getTime() < this._endTime) {
                                // We still have time, has the shard finished?
                                return !(iterator == null || iterator == undefined);
                            } else {
                                // Timeout
                                return false;
                            }
                        }.bind(this),

                        //..............................
                        function _eachIteratorFinish(err) {
                            next(err);
                        }.bind(this)

                    )
                
                }.bind(this))

            }.bind(this),

            //..............................
            // Callback that runs after last iteration of "_shardIterator"
            function _shardCallback(err) {

                if (this._onFinished) {
                    this._onFinished(err)
                }

            }.bind(this)
        )

    }.bind(this))

}
//::::::::::::::::::::::::::::::


//==============================
// PRIVATE METHODS
//==============================

//::::::::::::::::::::::::::::::
// Get all the shards in the stream
DynamoDBStream.prototype._getShards = function(callback) {

    // Params for describeStream function
    var params = {
        StreamArn: this._streamArn
    };

    // Get list of Shards and Shard iterators
    this._dynamodbstreams.describeStream(params, function(err, data) {

        // List of all shards in an array
        if (data && "StreamDescription" in data && "Shards" in data.StreamDescription) {
            callback(err, data.StreamDescription.Shards);
        } else {
            callback(err);
        }

    });

}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// Get the iterator for a shard
DynamoDBStream.prototype._getIterator = function(shard, callback) {

    // If the Shard is already in this._shardSequenceNumbers, generate iterator from the SequenceNumber
    // else generate the iterator using ShardIteratorType: 'TRIM_HORIZON'
    var params = {
        ShardId: shard.ShardId,
        StreamArn: this._streamArn
    };

    if (shard.ShardId in this._shardSequenceNumbers) {

        // Set params for generating shard iterator
        params.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
        params.SequenceNumber = this._shardSequenceNumbers[shard.ShardId];

    } else {

        // Set params to for getting shard iterator
        params.ShardIteratorType = 'TRIM_HORIZON';

    }

    this._dynamodbstreams.getShardIterator(params, function(err, data) {

        if (data && "ShardIterator" in data) {
            callback(err, data.ShardIterator);
        } else {
            callback(err);
        }

    });

}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// Get all the new records for a shard
DynamoDBStream.prototype._getRecords = function(iterator, callback) {

    var params = {
        ShardIterator: iterator
    };

    // Get Records for the shard
    this._dynamodbstreams.getRecords(params, function(err, data) {

        if (data) {
            var records = ("Records" in data) ? data.Records : null;
            var nextIterator = ("NextShardIterator" in data) ? data.NextShardIterator : null;
            callback(err, records, nextIterator);
        } else {
            callback(err);
        }

    });

}
//::::::::::::::::::::::::::::::


