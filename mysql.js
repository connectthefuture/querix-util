var mysql = require('mysql');
var nconf = require('./config');

var connections = {};
/**
* Instanciates every mysql connections as found in the configuration file
*/
var connectToDBs = function() {
  var productions = nconf.get('productions');
  for (var productionId in productions) {
    connections[productionId] = createProductionConnection(productionId);
  }
}

connectToDBs();

function createProductionConnection(productionId){
  var productions = nconf.get('productions');
  var config = productions[productionId].mysql;
  var connection = mysql.createConnection({
      host: config.host,
      port: config.port,
      user: config.username,
      password: config.password,
      database: config.database,
      multipleStatements: true
  });
  return connection;
}


/**
* Returns a mysql connection object corresponding to the given production id
*/
var mysqlConnection = function(productionId) {
  if (!connections[productionId])
    throw new Error('Production not found');
  return connections[productionId];
}

/**
* For test purposes only, resets the connection
*/
mysqlConnection.resetConnections = function() {
  for (key in connections) {
    connections[key].close;
  }
  connections = {};
  connectToDBs();
}

mysqlConnection.setConnection = function(key, connection) {
  connections[key] = connection;
}

mysqlConnection.createProductionConnection = createProductionConnection;

module.exports = exports = mysqlConnection;
