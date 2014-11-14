var amqp = require('amqp');
var nconf = require('./config');

var connections = {};
/**
* Instanciates every rabbitMQ connections as found in the configuration file
*/
var connectToQueues = function() {
  var productions = nconf.get('productions');
  for (var productionId in productions) {
    var config = productions[productionId].rabbit;
    if (config) {
      connections[productionId] = amqp.createConnection({
        host: config.host,
        port: config.port,
        user: config.username,
        password: config.password
      });
    }
  }
}

connectToQueues();

/**
* Returns a rabbitMQ connection object corresponding to the given production id
*/
var rabbitConnection = function(productionId) {
  if (!connections[productionId])
    throw new Error('Production not found');
  return connections[productionId];
}

/**
* For test purposes only, resets the connection
*/
rabbitConnection.resetConnections = function() {
  for (key in connections) {
    connections[key].close;
  }
  connections = {};
  connectToQueues();
}

rabbitConnection.setConnection = function(key, connection) {
  connections[key] = connection;
}

module.exports = exports = rabbitConnection;
