'use strict';

var mysql = require('mysql');


var MysqlClient = function(config){
    this.pool = mysql.createPool(config);
}


MysqlClient.prototype.closeConnectionPool = function(callback){
    this.pool.end(function(err){
        if(err) return callback(err);

        return callback();
    });
}


MysqlClient.prototype.resetConnectionPool = function(config, callback){
    var self = this;

    self.closeConnectionPool(function onClose(err){
        if(err) return callback(err);

        self.pool = mysql.createPool(config);

        return callback();
    });
}


MysqlClient.prototype.query = function(query, callback){
    var self = this;

    self.pool.getConnection(function(err, connection){
        if(err) return callback(err);

        var queryString;

        queryString = buildQueryString(query);

        connection.query(queryString, function(err, rows){
            if(err) return callback(err);

            connection.release();

            return callback(null, rows);
        });
    });
}


///////////////////////////////////////////////////////////////////////////////
/// utility functions

var buildQueryString = function(query){
    var result = "SELECT";

    if(query.distinct) result += " DISTINCT";

    for(var i in query.select){
        if(i !== 0) result += ", ";

        result += "\n\t" + select[i] + " as " + select[i].replace('.','_');
    }

    result += "\n";

    for(var i in query.from)
        result += query.from[i] + "\n";

    for(var i in query.where){
        if(i === 0) result += "WHERE\n\t";
        else        result += "\tAND ";

        result += query.where[i] + "\n";
    }

    result += ";";

    return result;
}


module.exports = exports = MysqlClient;
