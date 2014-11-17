'use strict';

var es = require('./elasticClient');
var mysql = require('./mysqlClient');


var ProductionClient = function(prodName, config){

    this.name = prodName;
    this.config = config;
    this.es = es(config.elasticsearch);
    this.mysql = mysql(config.mysql);
}


ProductionClient.prototype.closeMysqlConnection = function(callback){
    var self = this;

    self.mysql.closeConnectionPool(function onClose(err){
        if(err) return callback(err);

        return callback();
    });
}


ProductionClient.prototype.resetMysqlConnection = function(callback){
    var self = this;

    self.mysql.resetConnectionPool(self.config.mysql, function onReset(err){
        if(err) return callback(err);

        return callback();
    });
}


module.exports = exports = ProductionClient;
