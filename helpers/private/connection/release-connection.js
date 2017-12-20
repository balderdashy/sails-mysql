//  ██████╗ ███████╗██╗     ███████╗ █████╗ ███████╗███████╗
//  ██╔══██╗██╔════╝██║     ██╔════╝██╔══██╗██╔════╝██╔════╝
//  ██████╔╝█████╗  ██║     █████╗  ███████║███████╗█████╗
//  ██╔══██╗██╔══╝  ██║     ██╔══╝  ██╔══██║╚════██║██╔══╝
//  ██║  ██║███████╗███████╗███████╗██║  ██║███████║███████╗
//  ╚═╝  ╚═╝╚══════╝╚══════╝╚══════╝╚═╝  ╚═╝╚══════╝╚══════╝
//
//   ██████╗ ██████╗ ███╗   ██╗███╗   ██╗███████╗ ██████╗████████╗██╗ ██████╗ ███╗   ██╗
//  ██╔════╝██╔═══██╗████╗  ██║████╗  ██║██╔════╝██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
//  ██║     ██║   ██║██╔██╗ ██║██╔██╗ ██║█████╗  ██║        ██║   ██║██║   ██║██╔██╗ ██║
//  ██║     ██║   ██║██║╚██╗██║██║╚██╗██║██╔══╝  ██║        ██║   ██║██║   ██║██║╚██╗██║
//  ╚██████╗╚██████╔╝██║ ╚████║██║ ╚████║███████╗╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
//   ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═══╝╚══════╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
//
// Release an open database connection.

var MySQL = require('machinepack-mysql');

module.exports = function releaseConnection(connection, leased, cb) {
  // If this connection was leased outside of the Adapter, don't release it.
  if (leased) {
    return setImmediate(function ensureAsync() {
      return cb();
    });
  }

  MySQL.releaseConnection({
    connection: connection
  }).switch({
    error: function error(err) {
      return cb(new Error('There was an error releasing the connection back into the pool.' + err.stack));
    },
    badConnection: function badConnection() {
      return cb(new Error('Bad connection when trying to release an active connection.'));
    },
    success: function success() {
      return cb();
    }
  });
};
