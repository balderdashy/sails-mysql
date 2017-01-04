module.exports = {
  // Helpers for handling connections
  connection: {
    createManager: require('./connection/create-manager'),
    destroyManager: require('./connection/destroy-manager'),
    releaseConnection: require('./connection/release-connection'),
    spawnConnection: require('./connection/spawn-connection'),
    spawnOrLeaseConnection: require('./connection/spawn-or-lease-connection')
  },

  // Helpers for handling query logic
  query: {
    create: require('./query/create'),
    createEach: require('./query/create-each'),
    compileStatement: require('./query/compile-statement'),
    destroy: require('./query/destroy'),
    initializeQueryCache: require('./query/initialize-query-cache'),
    processEachRecord: require('./query/process-each-record'),
    preProcessRecord: require('./query/pre-process-record'),
    runNativeQuery: require('./query/run-native-query'),
    runQuery: require('./query/run-query'),
    update: require('./query/update')
  },

  // Helpers for dealing with underlying database schema
  schema: {
    buildIndexes: require('./schema/build-indexes'),
    buildSchema: require('./schema/build-schema'),
    escapeTableName: require('./schema/escape-table-name')
  }
};
