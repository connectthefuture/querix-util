'use strict';

var es = require('elasticsearch');
var config = require('nconf');


var elasticClient = function(config){
    this.connexion = new elasticsearch.Client({
        host: config.host + ':' + config.port;
        log: config.logLevel;
    });

    this.prodIndex = config.prodIndex;
}


module.exports = exports = elasticClient;
