//==============================
//SETUP
//==============================

const SHARD_CHECK_TIME = 60000;
const POLL_DELAY_TIME = 1000;

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
    this._pollingShards = {};

    // event handlers
    this._onRecord = null;      // To be called whenever a record is received
    this._onShardOpen = null;   // To be called whenever a new shard has been found
    this._onShardClose = null; // To be called whenever a shard has been drained
    this._onFinished = null;    // To be called at the very end

    // default limit, run forever
    this._endTime = null;

    // Once a minute check the shards for changes
    this._shardCheckTimer = setInterval(this._shardCheck.bind(this), SHARD_CHECK_TIME);
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
// Function that sets the "_onShardOpen" callback
DynamoDBStream.prototype.onShardOpen = function(callback) {
    this._onShardOpen = callback;
}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// Function that sets the "_onShardClose" callback
DynamoDBStream.prototype.onShardClose = function(callback) {
    this._onShardClose = callback;
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

    // Make sure we have default handlers for all the callbacks
    if (!this._onRecord) {
        this._onRecord = function(tableName, shard, record, next) { next() }
    }
    if (!this._onShardOpen) {
        this._onShardOpen = function(tableName, shard, next) { next() }
    }
    if (!this._onShardClose) {
        this._onShardClose = function(tableName, shard, next) { next() }
    }
    if (!this._onFinished) {
        this._onFinished = function(err) { }
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

                if (!(shard.ShardId in this._shardSequenceNumbers)) {
                    // This is a new shard
                    this._onShardOpen(this._tableName, shard, function() {
                        this._pollShard(shard, next);
                    }.bind(this))
                } else {
                    // This is an existing shard
                    this._pollShard(shard, next);
                }
                

            }.bind(this),

            //..............................
            // Callback that runs after last iteration of "_shardIterator"
            function _shardCallback(err) {

                // If we are finish polling all shards, then we are done with this stream
                if (Object.keys(this._pollingShards).length == 0) {

                    this._onFinished(err);
                
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

    if (shard.ShardId in this._shardSequenceNumbers && this._shardSequenceNumbers[shard.ShardId] != null) {

        // Set params for generating shard iterator
        params.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
        params.SequenceNumber = this._shardSequenceNumbers[shard.ShardId];

    } else {

        // Set params to for getting shard iterator
        params.ShardIteratorType = 'TRIM_HORIZON';
        this._shardSequenceNumbers[shard.ShardId] = null;

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


//::::::::::::::::::::::::::::::
// Polls the shard until the end of time.
DynamoDBStream.prototype._pollShard = function(shard, next) {

    // Ignore closed shards
    if (shard.ShardId in this._shardSequenceNumbers && this._shardSequenceNumbers[shard.ShardId] == "closed") {
        // console.log("Skipping closed shard: " + shard.ShardId)
        return next();
    }

    // Get the initial state of the iterator
    this._getIterator(shard, function(err, iterator) {

        if (err) return next(err);

        // Register that we are polling this shard
        this._pollingShards[shard.ShardId] = true;

        // KEep looping until the iterator runs dry
        async.doWhilst(

            //..............................
            // With each iterators grab all the records
            function _whileValidIterator(callback) {

                this._getRecords(iterator, function(err, records, nextIterator) {

                    // Store the next iterator if there is one
                    iterator = nextIterator;

                    var has_recorded_event = false;

                    // Process each record in order
                    async.eachSeries(records, 

                        //..............................
                        // Iterate over each record
                        function _recordEvent(record, next) {

                            has_recorded_event = true;
                            this._onRecord(this._tableName, shard, record, next);

                        }.bind(this),

                        //..............................
                        // All records have been drained from this iterator
                        function _recordCallback(err) {

                            // Delay the starting of a new iterator for a moment (reduces load)
                            setTimeout(function() {
                                callback(err);
                            }.bind(this), has_recorded_event ? 0 : POLL_DELAY_TIME)

                        }.bind(this)
                    )

                }.bind(this));
            }.bind(this),

            //..............................
            // Check if there are any more iterators to process ... or if we have run out of time
            function _whileValidIteratorTest() {
                if (this._endTime == null || new Date().getTime() < this._endTime) {
                    // We still have time, has the shard finished?
                    return !(iterator == null || iterator == undefined);
                } else {
                    // Timeout
                    return false;
                }
            }.bind(this),

            //..............................
            // All finished with this shard.
            function _whenIteratorRunsDry(err) {
                
                // We are finished polling this shard
                delete this._pollingShards[shard.ShardId];
                if (iterator == null || iterator == undefined) {
                    // This shard has closed
                    this._onShardClose(this._tableName, shard, function() {
                        next(err);
                    }.bind(this));
                } else {
                    // This process has timed out
                    next(err);                    
                }

            }.bind(this)

        )
    
    }.bind(this))
}
//::::::::::::::::::::::::::::::


//::::::::::::::::::::::::::::::
// Check for new shards in the stream
DynamoDBStream.prototype._shardCheck = function() {

    // Get all the shards
    this._getShards(function(err, shards) {

        if (err) return console.error(err);

        // Loop through all the shards and see if there are any new shards
        shards.forEach(function(shard) {

            if (!(shard.ShardId in this._shardSequenceNumbers)) {

                // We have a new shard
                this._onShardOpen(this._tableName, shard, function() {

                    // Start polling this new shard
                    this._pollShard(shard, function(err) {

                        // If we are finish polling all shards, then we are done with this stream
                        if (Object.keys(this._pollingShards).length == 0) {
                            this._onFinished(err)
                        }

                    }.bind(this));
                }.bind(this));
            }

        }.bind(this));

    }.bind(this));

}
//::::::::::::::::::::::::::::::


