//==============================
//SETUP
//==============================

// Libraries
var fs = require('fs'),
	AWS = require('aws-sdk'),
	async = require('async');


// Constants defining the level of parallelism (using async).
const GET_ITERATOR_LIMIT = 10;
const ON_RECORD_LIMIT = 10;

// Constants defining the wait periods between loops
const WAIT_AFTER_ERROR = 60000;
const WAIT_AFTER_NO_DATA = 5000;

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

function DynamoDBStream(config, streamArn, previousShards) {

    // Set AWS Config
    var creds = new AWS.Credentials(config.accessKeyId, config.secretAccessKey);

    // Initialise DynamoDB Object and DynamoDB Stream Object
    this._dynamodbstreams = new AWS.DynamoDBStreams({"credentials": creds, "region": config.region});

    // Attributes
    this._streamArn = streamArn;
    this._shardSequenceNumbers = (typeof previousShards == "object") ? previousShards : {};
    this._pollStreamInterval = 0; // Interval for calling getStreamData (0 seconds if data was returned last time, 5 seconds if no data was returned)

    // event handlers
    this._onRecord = null; // To be called whenever a record is received
    this._onSequenceNumber = null; // To be called whenever new records have been read from a shard

	// default limit, run forever
    this._endTime = null;

}
//::::::::::::::::::::::::::::::


//==============================
// PUBLIC METHODS
//==============================

// Function that sets the "_onRecord" callback
//::::::::::::::::::::::::::::::
DynamoDBStream.prototype.onRecord = function(callback) {

    this._onRecord = callback;

}
//::::::::::::::::::::::::::::::

// Function that sets the "_onRecord" callback
//::::::::::::::::::::::::::::::
DynamoDBStream.prototype.onSequenceNumber = function(callback) {

    this._onSequenceNumber = callback;

}
//::::::::::::::::::::::::::::::

// The main function which executes the other functions
//::::::::::::::::::::::::::::::
DynamoDBStream.prototype.Run = function(limit_seconds) {

    // Set end time for loop with a 10% wiggle
	if (limit_seconds) {
		var wiggle = limit_seconds * 0.1 * Math.random();
		var timeLimit = Math.round((limit_seconds - wiggle) * 1000);
		this._endTime = new Date().getTime() + timeLimit;
	}


    // EXECUTE
    //------------------------------
    this._getShards(function(err, shards) {

        // Set the interval to 5 (as if no data has been received)
        this._pollStreamInterval = WAIT_AFTER_NO_DATA;

        async.eachLimit(shards, GET_ITERATOR_LIMIT,
            // Function that iterates over each shard
            //..............................
            function _shardIterator(shard, next) {

                // console.log("\n\nProcessing shard ", shard.ShardId);
                this._getIterator(shard, function(err, iterator) {

                    // console.log(iterator);
                    this._getRecords(iterator, function(err, records) {

                        // console.log(records);
                        async.eachLimit(records, ON_RECORD_LIMIT,
                            // Function that iterates over each record
                            //..............................
                            function _recordIterator(record, next) {

                                this._onRecord(record, next);

                            }.bind(this),
                            //..............................

                            // Callback that runs after last iteration of "_recordIterator"
                            //..............................
                            function _recordCallback(err) {

                                if (err) {
                                    console.error(err);
                                    next(err);
                                } else if (records.length > 0) {

                                    // console.log("Finished reading records");

                                    // Set interval to 0 if any data is received
                                    this._pollStreamInterval = 0;
                                    this._shardSequenceNumbers[shard.ShardId] = records[records.length - 1].dynamodb.SequenceNumber;
                                    this._onSequenceNumber(shard.ShardId, records[records.length - 1].dynamodb.SequenceNumber, next);

                                } else {
                                    next(err);
                                }

                            }.bind(this)
                            //..............................
                        );

                    }.bind(this));

                }.bind(this));

            }.bind(this),
            //..............................

            // Callback that runs after last iteration of "_shardIterator"
            //..............................
            function _shardCallback(err) {

                if (err) {
                    console.log(err);
                    this._pollStreamInterval = WAIT_AFTER_ERROR;
                } else {
                    // console.log("Finished processing shards");
                }

                // Delay before starting the next loop
                if (this._endTime == null || new Date().getTime() < this._endTime) {
                    setTimeout(this.Run.bind(this), this._pollStreamInterval)
                    // console.log("Looping around for another run in " + (this._pollStreamInterval/1000) + "s")
                } else {
                    // console.log("Finished!")
                }

            }.bind(this)
            //..............................
        );

    }.bind(this));
    //------------------------------

}
//::::::::::::::::::::::::::::::

//==============================
// PRIVATE METHODS
//==============================

// Get all the shards in the stream
//::::::::::::::::::::::::::::::
DynamoDBStream.prototype._getShards = function(callback) {

    // Params for describeStream function
    var params = {
        StreamArn: this._streamArn
    };

    // Get list of Shards and Shard iterators
    this._dynamodbstreams.describeStream(params, function(err, data) {

        if (err) console.error(err);

        // List of all shards in an array
        var shards = data.StreamDescription.Shards;
        callback(err, shards);

    });

}
//::::::::::::::::::::::::::::::

// Get the iterator for a shard
//::::::::::::::::::::::::::::::
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

        if (err) {
            console.error(err);
        }
        callback(err, data.ShardIterator);

    });

}
//::::::::::::::::::::::::::::::

// Get all the new records for a shard
//::::::::::::::::::::::::::::::
DynamoDBStream.prototype._getRecords = function(iterator, callback) {

    var params = {
        ShardIterator: iterator
    };

    // Get Records for the shard
    this._dynamodbstreams.getRecords(params, function(err, data) {

        if (err) {
            console.error(err);
        }
        callback(err, data.Records);

    });

}
//::::::::::::::::::::::::::::::


//==============================
//PROGRAM CODE
//==============================
