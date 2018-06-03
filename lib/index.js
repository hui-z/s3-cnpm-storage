'use strict';

var fs = require('fs');
var url = require('url')
var AWS = require('aws-sdk');
var thunkify = require('thunkify-wrap');

function getS3Key(key) {
  // remove leading slash of key
  if (key && key[0] === '/') {
    key = key.slice(1)
  }
  return key;
}

function encodeSpecialCharacters(filename) {
  // Note: these characters are valid in URIs, but S3 does not like them for
  // some reason.
  return encodeURI(filename).replace(/[!'()* ]/g, function (char) {
    return '%' + char.charCodeAt(0).toString(16);
  });
}

function getUrlFormatter(bucket, endpoint) {
  return function formatUrl(key) {
    var parts = {
      protocol: 'https:',
      hostname: bucket + '.' + endpoint,
      pathname: '/' + getS3Key(encodeSpecialCharacters(key)),
    };
    return url.format(parts);
  }
}

function readFileThunk(path) {
  return new Promise(function (resolve, reject) {
    fs.readFile(path, function (err, data ) {
      if (err) reject(err)
      resolve(data)
    })
  })
}

/**
 * Expose `Client`
 */

module.exports = S3Storage;

/**
 * qn cnpm wrapper
 * @param {Object} options for qn client
 */
function S3Storage(options) {
  if (!(this instanceof S3Storage)) {
    return new S3Storage(options);
  }

  options = options || {}

  s3Options = {}

  this.endpoint = options.endpoint
  this.bucket = options.bucket
  this.formatUrl = getUrlFormatter(this.bucket, this.endpoint)

  this.client = options.client || new AWS.S3({
    accessKeyId: options.accessKeyId,
    secretAccessKey: options.secretAccessKey,
    region: options.region,
    params: {
      Bucket: this.bucket
    }
  });
  thunkify(this.client, ['putObject', 'deleteObject', 'upload', 'getObject']);
}

/**
 * Upload file
 *
 * @param {String} filepath
 * @param {Object} options
 *  - {String} key
 *  - {Number} size
 */
S3Storage.prototype.upload = function* (filepath, options) {
  var key = getS3Key(options.key)
  try {
    yield this.client.deleteObject({ Key: key });
  } catch (err) {
    // ignore error here
  }

  var content = yield readFileThunk(filepath)
  var res = yield this.client.upload({
    Key: key,
    Body: content,
    ACL: 'public-read'
  });

  return { url: this.formatUrl(options.key) };
};

S3Storage.prototype.uploadBuffer = function* (buf, options) {
  var key = getS3Key(options.key)
  try {
    yield this.client.deleteObject({ Key: key });
  } catch (err) {
    // ignore error here
  }

  var res = yield this.client.upload({
    Key: key,
    Body: buf,
    ACL: 'public-read'
  });
  return { url: this.formatUrl(options.key) };
};

S3Storage.prototype.url = function (key) {
  return this.formatUrl(key);
};

S3Storage.prototype.download = function* (key, filepath, options) {
  key = getS3Key(key)
  yield data = this.client.getObject({ Key: key })
  var writeStream = fs.createWriteStream(filepath)
  writeStream.write(data)
  writeStream.end()
};

S3Storage.prototype.remove = function* (key) {
  key = getS3Key(key)
  try {
    return yield this.client.deleteObject({ Key: key });
  } catch (err) {
    throw err;
  }
};
