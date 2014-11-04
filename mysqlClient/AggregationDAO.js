var fs = require('fs');
var async = require('async');
var _ = require('underscore');

var log = require('log4js').getLogger('AggregationDAO');

/**
* Instanciates the DAO, recuperates the
*/
var AggregationDAO = function(prodId, config) {
  var that = this;
  this.mysql = require('querix-util/mysql')(prodId);
  this.prodId = prodId;
  this.config = config;

  this.listSQL = fs.readFileSync(this.config.listSQL);
  this.aggregationSQL = [];
  var files = fs.readdirSync(this.config.aggregationSQL);
  files.forEach(function(filename) {
    that.aggregationSQL.push(fs.readFileSync(that.config.aggregationSQL + filename));
  });
}

var d2 = function(int) {
  return ("0" + int).slice(-2);
}

var parseValidContent = function(content, isdipdate) {
  if (content instanceof Date) {
    return content.getFullYear() + '-' + d2((content.getMonth() + 1)) + '-' + d2(content.getDate()) + 'T' + d2(content.getHours()) + ':' + d2(content.getMinutes()) + ':' + d2(content.getSeconds());
  }
  if (content === "0000-00-00 00:00:00" || content === '0000-00-00' || Number(content) > 9223372036854775807)
    return null;
  else return content;
}

/**
* Retreives all rows from a table (e.g. campaigns)
*/
AggregationDAO.prototype.getAll = function(cb) {
  var sql = String(this.listSQL);
  sql = String(sql).replace(/WHERE ([a-zA-Z_\.]+) = \?/, '').replace(/AND [a-zA-Z_\.]+ = \?/, '');
  log.debug('SQL Query : ', sql);
  this.mysql.query(sql, function(err, objects, fields) {
    if (err) return cb(err);
    objects.forEach(function(object) {
      for (var key in object) {
        object[key] = parseValidContent(object[key]);
      }
    });
    cb(null, objects, fields);
  });
}

/**
* Retreives a single row from a table (e.g. campaign)
*/
AggregationDAO.prototype.getObjectById = function(id, cb) {
  log.debug('SQL Query : ', String(this.listSQL));
  this.mysql.query(String(this.listSQL), [id], function(err, object) {
    log.debug('SQL Result : ', object);
    if (err) return cb(err);
    if (!object || object.length < 1) return cb(new Error('Object not found'));
    for (var key in object) {
      object[key] = parseValidContent(object[key]);
    }
    cb(null, object[0]);
  });
}

/**
* Using a row from a table (e.g. a campaign), retreives all aggregation from other tables (e.g. using inventory_data to get a sum of actual_imps)
*/
AggregationDAO.prototype.aggregateObject = function(object, cb) {
  var that = this;
  async.eachSeries(this.aggregationSQL, function(sql, done) {
    sql = String(sql);
    log.debug('SQL Query : ', sql);
    var count = sql.match(/\?/g);
    count = count ? count.length : 0;
    var params = [];
    for (var i = 0; i < count; i++) {
      params.push(object.id);
    }
    that.mysql.query(sql, params, function(err, results) {
      log.debug('SQL Results : ', results);
      if (err) return done(err);
      var result = results[0];
      if (!result) return done();
      for (var key in result) {
        object[key] = parseValidContent(result[key]);
      }
      done();
    });
  }, function(err) {
    if(err) return cb(err);
    cb(null, object);
  });
}

/**
* Binary search for the object with the given id in the list
*/
var bfindId = function(list, id) {
  var a = 0;
  var b = list.length;
  var middle = Math.floor((a+b)/2);
  while(list[middle].id !== id && a !== middle) {
    if (list[middle].id < id)
      a = middle;
    else
      b = middle;
    middle = Math.floor((a+b)/2);
  }
  if (list[middle].id !== id)
    return null;

  return list[middle];
}

/**
* Retrieves aggregations for all objects, and merges them with the corresponding object
*/
AggregationDAO.prototype.aggregateAllObjects = function(objects, cb) {
  var that = this;
  async.eachSeries(this.aggregationSQL, function(sql, done) {
    sql = String(sql).replace(/WHERE ([a-zA-Z_\.]+) = \?/g, '').replace(/AND [a-zA-Z_\.]+ = \?/g, '');
    log.debug('SQL Query : ', String(sql));
    that.mysql.query(String(sql), function(err, results) {
      if (err) return done(err);
      log.debug('SQL Query Completed Successfully');
      results.forEach(function(result) {
        var object = bfindId(objects, result.id);
        if (object) {
          if (object.id % 1000 === 0)
            log.debug('Processing object :', object.id);
          for (var key in result) {
            if (key !== 'id')
              object[key] = parseValidContent(result[key]);
          }
        }
      });
      log.debug('SQL Query Processing Completed');
      done();
    });
  }, function(err) {
    if(err) return cb(err);
    cb(null, objects);
  });
}

module.exports = exports = AggregationDAO;
