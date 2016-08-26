var AWS = require('aws-sdk');
var S3StreamDownload = require('./index');
var fs = require('fs');

var s3 = new AWS.S3();

var s3StreamDownload = new S3StreamDownload(s3, {Bucket: '<BUCKET>', Key:'<KEY>'}, {downloadChunkSize:  1024 * 1024, concurrentChunks: 7});

s3StreamDownload.pipe(fs.createWriteStream('<FILENAME>'));

s3StreamDownload.on('end', function() {
    process.exit(0);
});