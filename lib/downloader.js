'use strict';
var util = require("util");
var EventEmitter = require("events").EventEmitter;
var async = require('async');
var debug = require('debug')('s3-stream-download:downloader');

/**
 * Size of chunk to download.  Need to tradeoff chattiness vs memory
 * usage of buffering the chunk
 */

const DOWNLOAD_CHUNK_SIZE = 5 * 1024 * 1024; // 5MB
const CONCURRENT_CHUNKS = 5;
const RETRIES = 5;

/**
 * Constructor for a `Downloader`. Takes a `AWS.S3` client
 * instance, and additional object configuration to be
 * passed into the client's `createMultipartUpload` method.
 *
 * @params {AWS.S3} client
 * @params {Object} config
 */

module.exports = class Downloader extends EventEmitter {
    constructor(s3Client, s3Params, options) {
        super();
        this._s3Client = s3Client;
        this._s3Params = s3Params;
        this._nextPartNumber = 0;
        this._started = false;
        // Will download concurrentChunks and then pause until first read sets to true
        this._paused = true;
        this._emitting = false;
        this._download_chunk_size = options.downloadChunkSize || DOWNLOAD_CHUNK_SIZE;
        this._concurrent_chunks = options.concurrentChunks || CONCURRENT_CHUNKS;
        this._retries = options.retries || RETRIES;
        this._parts = {};

        if (!s3Params.Bucket) {
            throw new Error("Downloader requires options with `Bucket` specified.");
        }

        if (!s3Params.Key) {
            throw new Error("Downloader requires options with `Key` specified.");
        }

        this._startDownload();
    }

    get paused() {
        return this._paused;
    }

    set paused(paused) {
        if(this._paused!==paused) {
            debug('paused: ' + paused);
        }
        this._paused = paused;
        this._emitParts();
    }

    _startDownload() {
        if(!this._started) {
            var self = this;
            self._started = true;
            debug('starting download');
            self._s3Client.listObjects({Bucket: self._s3Params.Bucket, Prefix: self._s3Params.Key}, function(err, data) {
                if(err) {
                    debug('listObjects err: ' + err);
                    return self.emit('error', err);
                }
                else {
                    if(!data || !data.Contents || data.Contents.length === 0) {
                        debug('key not found bucket: ' + self._s3Params.Bucket + ', Key: ' + self._s3Params.Key);
                        return self.emit('error', 'Key not found');
                    }
                    else {
                        self.totalObjectSize = data.Contents[0].Size;
                        debug('totalObjectSize: ' + self.totalObjectSize);
                        self.totalParts = Math.ceil(self.totalObjectSize / self._download_chunk_size);
                        debug('totalParts: ' + self.totalParts);

                        async.timesLimit(self.totalParts, self._concurrent_chunks,
                            function(partNumber, done) {
                                var startByte = partNumber * self._download_chunk_size;
                                var endByte = startByte + self._download_chunk_size - 1;
                                if(endByte > self.totalObjectSize) {
                                    endByte = self.totalObjectSize;
                                }
                                debug(`partNumber: ${partNumber}, startByte: ${startByte}, endByte: ${endByte}`);

                                async.retry({
                                        times: self._retries,
                                        interval: function(retryCount) {
                                            debug(`Retrying partNumber:${partNumber}, retryCount:${retryCount}`);
                                            return 1000 * Math.pow(2, retryCount);
                                        }
                                    },
                                    function(done) {
                                        var params = util._extend({Range:`bytes=${startByte}-${endByte}`}, self._s3Params);
                                        self._s3Client.getObject(params, function(err, data) {
                                            if(err) {
                                                debug('getObject error: ' + err);
                                                self.emit('error', err);
                                            }
                                            else {
                                                self._parts[partNumber] = data.Body;
                                                debug('received partNumber: ' + partNumber);
                                                self._emitParts();
                                            }
                                            setImmediate(() => done(err));
                                        });
                                    },
                                    function(err) {
                                        if(self.paused) {
                                            debug('pausing after download partNumber: ' + partNumber);
                                            var _checkIntervalId = setInterval(function() {
                                                if(!self.paused && _checkIntervalId) {
                                                    clearInterval(_checkIntervalId);
                                                    _checkIntervalId = undefined;
                                                    debug('resuming after download partNumber: ' + partNumber);
                                                    return done(err);
                                                }
                                            }, 100);
                                        }
                                        else {
                                            setImmediate(() => done(err));
                                        }
                                    }
                                );
                            },
                            function(err, results) {
                                if(err) {
                                    debug('download err: ' + err);
                                    self.emit('error', err);
                                }
                            }
                        );
                    }
                }
            });
        }
    }

    _emitParts() {
        if(!this._paused && !this._emitting) {
            var self = this;
            self._emitting = true;
            debug('emitting parts');
            async.whilst(
                function () {
                    return self._parts[self._nextPartNumber] && !self._paused;
                },
                function (done) {
                    var part = self._parts[self._nextPartNumber];
                    delete self._parts[self._nextPartNumber];
                    self._nextPartNumber++;
                    self.emit('part', part);
                    debug('emitting partNumber: ' + (self._nextPartNumber - 1));
                    if(self._nextPartNumber >= self.totalParts) {
                        debug('emitting finish');
                        self.emit('finish');
                    }
                    setImmediate(done);
                },
                function(err) {
                    self._emitting = false;
                }
            );

        }
    }
};

