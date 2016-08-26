var Downloader = require("./lib/downloader");
var DownloadStream = require('./lib/download-stream');

/**
 * Expose `S3StreamDownload`.
 */

module.exports = S3StreamDownload;

/**
 * Creates a stream for download via multipart stream to S3.
 *
 * @params {S3} s3
 * @params {Object} s3Params
 * @params {Object} options
 */

function S3StreamDownload (s3, s3Params, options) {
    var downloader = new Downloader(s3, s3Params, options);
    return new DownloadStream(downloader);
}
