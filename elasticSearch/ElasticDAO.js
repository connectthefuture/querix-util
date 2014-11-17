/**
* DAO allowing the inserting and bulk inserting of documents inside an ElasticSearch index/type
*/

var request = require('superagent');
var async = require('async');
var log = require('log4js').getLogger('ElasticDAO');
var fs = require('fs');

var BULK_SIZE = 10000;

var INDEX_ALREADY_EXISTS_EXCEPTION = "IndexAlreadyExistsException";

/**
* Instanciation of the DAO, and creation of the basic routes using the config object (passed from settings.json)
*/
var ElasticDAO = function(config) {
  this.config = config;
  this.baseUri = config.host + ':' + config.port + '/' + config.index + '/' + config.type + '/';
  this.serverUri = config.host + ':' + config.port + '/';
  this.bound = {};
}

/**
* Inserts (or updates) a document on elastic search at the given id
*/
ElasticDAO.prototype.save = function(document,id,callback){
  var buf = JSON.stringify(document);
  log.debug('Elastic Request : ',buf, '('+Buffer.byteLength(buf)+' Bytes)');

  request.put(this.baseUri+id).send(buf)
    .set('Content-Type', 'application/octet-stream')
    .set('Connection', 'close')
    .set('Transfer-Encoding', 'chunked')
    .end(function(err, result) {
      if (err) return callback(err);
      log.debug('Elastic Result : ' + result.text);
      if (result.status >= 400)
        return callback(new Error('Error while indexing'));
      callback(null, result);
  });
}

/**
* Bulk inserts (or updates) a document on elastic search using the 'id' field of the documents
*/
ElasticDAO.prototype.bulk = function(documents, callback) {
  var that = this;
  async.whilst(function() {
    return (documents.length > 0);
  }, function(next) {
    var output = '';
    var bulk = documents.splice(0, BULK_SIZE);
    bulk.forEach(function(doc) {
      output += JSON.stringify({index:{"_index": that.config.index, "_type": that.config.type, "_id": doc.id}}) + '\n';
      output += JSON.stringify(doc) + '\n';
    });
    log.debug('Elastic Bulk Request :', Buffer.byteLength(output), 'Bytes');
    request.post(that.serverUri+'_bulk').send(output)
      .set('Content-Type', 'application/octet-stream')
      .set('Connection', 'close')
      .set('Transfer-Encoding', 'chunked')
      .end(function(err, result) {
        if (err) return next(err);
        if (result.body.errors === true) {
          fs.writeFileSync('result.log', result.text);
          return next(new Error('An elastic error occured, check result.log for more informations'));
        }

        if (result.status >= 400)
          return next(new Error('Error while indexing'));
        log.debug('Elastic Request Successful');
        next();
    });
  }, callback);
}

/**
* Puts a mapping in Elastic
*/
ElasticDAO.prototype.putMapping = function(mapping, done) {
  log.debug('Putting mapping :', JSON.stringify(mapping, null, '\t'));
  //process.exit(0);
  request.put(this.serverUri+this.config.index+'/_mapping/'+this.config.type)
    .send(mapping)
    .end(function(err, result) {
      if (err) return done(err);
      if (result.status >= 400) return done(new Error('Could not put mapping !'));
      return done(null, result);
    });
}

/**
* Creates an Elastic Mapping
*/
ElasticDAO.prototype.makeMapping = function(searchableFields) {
  var mapping = {};
  mapping[this.config.type] = {properties: {}};
  var properties = mapping[this.config.type].properties;
  if (searchableFields)
    searchableFields.forEach(function(field) {
      properties[field] = {
        type: "string",
        fields: {
          lowercase: {
            type: "string",
            analyzer: "split_on_comma_lowercase"
          }
        }
      }
    });
  return mapping;
}

/**
* Deletes a whole index and its mapping in Elastic
*/
ElasticDAO.prototype.deleteMapping = function(done) {
  var self = this;
  request.del(this.baseUri)
    .end(function(err, result) {
      if (err) return done(err);
      if (result.status >= 400 && result.status != 404) {
        return done(result.error || new Error('Error while deleting type', self.config.index));
      }
      done(null, result);
    });
}

/**
* Creates the index and changes the default analyzer to be a "split on comma"
*/
ElasticDAO.prototype.putAnalyzer = function(done) {
  var self = this;
  var mapping = {
    settings: {
      analysis: {
        analyzer: {
          "default": {
            type: "pattern",
            pattern: ",",
            lowercase: false
          },
          "split_on_comma_lowercase": {
            type: "pattern",
            pattern: ",",
            lowercase: true
          }
        }
      }
    },
    "_default_": {
      dynamic_date_formats: ["date_hour_minute_second", "date_optional_time"]
    }
  }
  request.put(this.serverUri + this.config.index)
    .send(mapping)
    .end(function(err, result) {
      if (err) return done(err);
      if (result.status >= 400) {
        if (result.body.error && (String(result.body.error).substring(0, INDEX_ALREADY_EXISTS_EXCEPTION.length) === INDEX_ALREADY_EXISTS_EXCEPTION)) return done(new Error('Index Already Exists ('+self.config.index+')'))
        return done(result.error || new Error('Error while creating the index'));
      }
      done(null, result);
    });
}

/**
* Deletes the ElasticSearch index
*/
ElasticDAO.prototype.deleteIndex = function(done) {
  request.del(this.serverUri + this.config.index)
    .end(function(err, result) {
      if (err) return done(err);
      if (result.status >= 400 && result.status !== 404) {
        return done(result.error || new Error('Error while deleting the index'));
      }
      done(null, result);
    });
}

module.exports = exports = ElasticDAO;
