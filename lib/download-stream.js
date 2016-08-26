'use strict';
const Readable = require('stream').Readable;
var debug = require('debug')('s3-stream-download:download-stream');

module.exports = class DownloadStream extends Readable {
    constructor(downloader) {
        // Calls the stream.Readable(options) constructor
        super({});
        var self = this;
        this._downloader = downloader;

        downloader.on('part', function(part) {
            debug('received part');
            downloader.paused = !self.push(part);
        });

        downloader.on('error', function(err) {
            debug('downloader err: ' + err);
        });

        downloader.on('finish', function() {
            debug('finished');
            self.push(null);
        });
    }

    _read(size) {
        debug('read');
        this._downloader.paused = false;
    }
};