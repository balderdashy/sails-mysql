//  ███████╗██████╗  █████╗ ██╗    ██╗███╗   ██╗     ██████╗ ██████╗     ██╗     ███████╗ █████╗ ███████╗███████╗
//  ██╔════╝██╔══██╗██╔══██╗██║    ██║████╗  ██║    ██╔═══██╗██╔══██╗    ██║     ██╔════╝██╔══██╗██╔════╝██╔════╝
//  ███████╗██████╔╝███████║██║ █╗ ██║██╔██╗ ██║    ██║   ██║██████╔╝    ██║     █████╗  ███████║███████╗█████╗
//  ╚════██║██╔═══╝ ██╔══██║██║███╗██║██║╚██╗██║    ██║   ██║██╔══██╗    ██║     ██╔══╝  ██╔══██║╚════██║██╔══╝
//  ███████║██║     ██║  ██║╚███╔███╔╝██║ ╚████║    ╚██████╔╝██║  ██║    ███████╗███████╗██║  ██║███████║███████╗
//  ╚══════╝╚═╝     ╚═╝  ╚═╝ ╚══╝╚══╝ ╚═╝  ╚═══╝     ╚═════╝ ╚═╝  ╚═╝    ╚══════╝╚══════╝╚═╝  ╚═╝╚══════╝╚══════╝
//
// Returns either the leased connection that was passed in to the meta input of
// a helper or spawns a new connection. This is a normalized helper so the actual
// helper methods don't need to deal with the branching logic.

var _ = require('@sailshq/lodash');
var spawnConnection = require('./spawn-connection');

module.exports = function spawnOrLeaseConnection(datastore, meta, cb) {
  if (!_.isUndefined(meta) && _.has(meta, 'leasedConnection')) {
    return setImmediate(function ensureAsync() {
      cb(null, meta.leasedConnection);
    });
  }

  // Otherwise spawn the connection
  spawnConnection(datastore, cb);
};
