//  ███████╗ █████╗ ██╗██╗     ███████╗    ███╗   ███╗██╗   ██╗███████╗ ██████╗ ██╗
//  ██╔════╝██╔══██╗██║██║     ██╔════╝    ████╗ ████║╚██╗ ██╔╝██╔════╝██╔═══██╗██║
//  ███████╗███████║██║██║     ███████╗    ██╔████╔██║ ╚████╔╝ ███████╗██║   ██║██║
//  ╚════██║██╔══██║██║██║     ╚════██║    ██║╚██╔╝██║  ╚██╔╝  ╚════██║██║▄▄ ██║██║
//  ███████║██║  ██║██║███████╗███████║    ██║ ╚═╝ ██║   ██║   ███████║╚██████╔╝███████╗
//  ╚══════╝╚═╝  ╚═╝╚═╝╚══════╝╚══════╝    ╚═╝     ╚═╝   ╚═╝   ╚══════╝ ╚══▀▀═╝ ╚══════╝
//
// An adapter for MySQL and Waterline

var _ = require('@sailshq/lodash');
var async = require('async');
var Helpers = require('../helpers');

module.exports = (function sailsMySQL() {
  // Keep track of all the datastores used by the app
  var datastores = {};

  // Keep track of all the connection model definitions
  var modelDefinitions = {};

  var adapter = {
    identity: 'sails-mysql',

    // Waterline Adapter API Version
    adapterApiVersion: 1,

    defaults: {
      host: 'localhost',
      port: 3306,
      schema: true
    },

    //  ╔═╗═╗ ╦╔═╗╔═╗╔═╗╔═╗  ┌─┐┬─┐┬┬  ┬┌─┐┌┬┐┌─┐
    //  ║╣ ╔╩╦╝╠═╝║ ║╚═╗║╣   ├─┘├┬┘│└┐┌┘├─┤ │ ├┤
    //  ╚═╝╩ ╚═╩  ╚═╝╚═╝╚═╝  ┴  ┴└─┴ └┘ ┴ ┴ ┴ └─┘
    //  ┌┬┐┌─┐┌┬┐┌─┐┌─┐┌┬┐┌─┐┬─┐┌─┐┌─┐
    //   ││├─┤ │ ├─┤└─┐ │ │ │├┬┘├┤ └─┐
    //  ─┴┘┴ ┴ ┴ ┴ ┴└─┘ ┴ └─┘┴└─└─┘└─┘
    // This allows outside access to the connection manager.
    datastores: datastores,


    //  ╦═╗╔═╗╔═╗╦╔═╗╔╦╗╔═╗╦═╗  ┌┬┐┌─┐┌┬┐┌─┐┌─┐┌┬┐┌─┐┬─┐┌─┐
    //  ╠╦╝║╣ ║ ╦║╚═╗ ║ ║╣ ╠╦╝   ││├─┤ │ ├─┤└─┐ │ │ │├┬┘├┤
    //  ╩╚═╚═╝╚═╝╩╚═╝ ╩ ╚═╝╩╚═  ─┴┘┴ ┴ ┴ ┴ ┴└─┘ ┴ └─┘┴└─└─┘
    // Register a datastore config and generate a connection manager for it.
    registerDatastore: function registerDatastore(datastoreConfig, models, cb) {
      var identity = datastoreConfig.identity;
      if (!identity) {
        return cb(new Error('Invalid datastore config. A datastore should contain a unique identity property.'));
      }

      try {
        Helpers.registerDataStore({
          identity: identity,
          config: datastoreConfig,
          models: models,
          datastores: datastores,
          modelDefinitions: modelDefinitions
        }).execSync();
      } catch (e) {
        setImmediate(function done() {
          return cb(e);
        });
        return;
      }

      setImmediate(function done() {
        return cb();
      });
    },


    //  ╔╦╗╔═╗╔═╗╦═╗╔╦╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //   ║ ║╣ ╠═╣╠╦╝ ║║║ ║║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //   ╩ ╚═╝╩ ╩╩╚══╩╝╚═╝╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    // Destroy a manager and close any connections in it's pool.
    teardown: function teardown(identity, cb) {
      var datastoreIdentities = [];

      // If no specific identity was sent, teardown all the datastores
      if (!identity || identity === null) {
        datastoreIdentities = datastoreIdentities.concat(_.keys(datastores));
      } else {
        datastoreIdentities.push(identity);
      }

      // Teardown each datastore identity manager
      async.eachSeries(datastoreIdentities, function teardownDatastore(datastoreIdentity, next) {
        Helpers.teardown({
          identity: datastoreIdentity,
          datastores: datastores,
          modelDefinitions: modelDefinitions
        }).switch({
          error: function error(err) {
            return next(err);
          },
          success: function success() {
            return next();
          }
        });
      }, function asyncCb(err) {
        cb(err);
      });
    },


    //  ██████╗  ██████╗ ██╗
    //  ██╔══██╗██╔═══██╗██║
    //  ██║  ██║██║   ██║██║
    //  ██║  ██║██║▄▄ ██║██║
    //  ██████╔╝╚██████╔╝███████╗
    //  ╚═════╝  ╚══▀▀═╝ ╚══════╝
    //
    // Methods related to manipulating data stored in the database.


    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   ├┬┘├┤ │  │ │├┬┘ ││
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  ┴└─└─┘└─┘└─┘┴└──┴┘
    // Add a new row to the table
    create: function create(datastoreName, query, cb) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.create({
        datastore: datastore,
        models: models,
        query: query
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        notUnique: function error(errInfo) {
          var e = new Error(errInfo.message);
          e.footprint = errInfo.footprint;
          return cb(e);
        },
        success: function success(report) {
          var record = report && report.record || undefined;
          return cb(undefined, record);
        }
      });
    },


    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ╔═╗╔═╗╔═╗╦ ╦  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   ║╣ ╠═╣║  ╠═╣  ├┬┘├┤ │  │ │├┬┘ ││
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  ╚═╝╩ ╩╚═╝╩ ╩  ┴└─└─┘└─┘└─┘┴└──┴┘
    // Add multiple new rows to the table
    createEach: function createEach(datastoreName, query, cb) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.createEach({
        datastore: datastore,
        models: models,
        query: query
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        notUnique: function error(errInfo) {
          var e = new Error(errInfo.message);
          e.footprint = errInfo.footprint;
          return cb(e);
        },
        success: function success(report) {
          var records = report && report.records || undefined;
          return cb(undefined, records);
        }
      });
    },


    //  ╔═╗╔═╗╦  ╔═╗╔═╗╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╚═╗║╣ ║  ║╣ ║   ║   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩═╝╚═╝╚═╝ ╩   └─┘└└─┘└─┘┴└─ ┴
    // Select Query Logic
    find: function find(datastoreName, query, cb) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.select({
        datastore: datastore,
        models: models,
        query: query
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report.records);
        }
      });
    },


    //  ╦ ╦╔═╗╔╦╗╔═╗╔╦╗╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║ ║╠═╝ ║║╠═╣ ║ ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╩  ═╩╝╩ ╩ ╩ ╚═╝  └─┘└└─┘└─┘┴└─ ┴
    // Update one or more models in the table
    update: function update(datastoreName, query, cb) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.update({
        datastore: datastore,
        models: models,
        query: query
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        notUnique: function error(errInfo) {
          var e = new Error(errInfo.message);
          e.footprint = errInfo.footprint;
          return cb(e);
        },
        success: function success(report) {
          if (report) {
            return cb(undefined, report.records);
          }

          return cb();
        }
      });
    },


    //  ╔╦╗╔═╗╔═╗╔╦╗╦═╗╔═╗╦ ╦  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //   ║║║╣ ╚═╗ ║ ╠╦╝║ ║╚╦╝  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ═╩╝╚═╝╚═╝ ╩ ╩╚═╚═╝ ╩   └─┘└└─┘└─┘┴└─ ┴
    // Delete one or more records in a table
    destroy: function destroy(datastoreName, query, cb) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.destroy({
        datastore: datastore,
        models: models,
        query: query
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          if (report) {
            return cb(undefined, report.records);
          }

          return cb();
        }
      });
    },


    //  ╔╗╔╔═╗╔╦╗╦╦  ╦╔═╗   ┬┌─┐┬┌┐┌  ┌─┐┬ ┬┌─┐┌─┐┌─┐┬─┐┌┬┐
    //  ║║║╠═╣ ║ ║╚╗╔╝║╣    ││ │││││  └─┐│ │├─┘├─┘│ │├┬┘ │
    //  ╝╚╝╩ ╩ ╩ ╩ ╚╝ ╚═╝  └┘└─┘┴┘└┘  └─┘└─┘┴  ┴  └─┘┴└─ ┴
    // Build up native joins to run on the adapter.
    join: function join(datastoreName, query, cb) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.join({
        datastore: datastore,
        models: models,
        query: query
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report);
        }
      });
    },


    //  ╔═╗╦  ╦╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠═╣╚╗╔╝║ ╦  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩ ╩ ╚╝ ╚═╝  └─┘└└─┘└─┘┴└─ ┴
    // Find out the average of the query.
    avg: function avg(datastoreName, query, cb) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.avg({
        datastore: datastore,
        models: models,
        query: query
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report);
        }
      });
    },


    //  ╔═╗╦ ╦╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╚═╗║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩ ╩  └─┘└└─┘└─┘┴└─ ┴
    // Find out the sum of the query.
    sum: function sum(datastoreName, query, cb) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.sum({
        datastore: datastore,
        models: models,
        query: query
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report);
        }
      });
    },


    //  ╔═╗╔═╗╦ ╦╔╗╔╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║  ║ ║║ ║║║║ ║   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╚═╝╝╚╝ ╩   └─┘└└─┘└─┘┴└─ ┴
    // Return the number of matching records.
    count: function count(datastoreName, query, cb) {
      var datastore = datastores[datastoreName];
      var models = modelDefinitions[datastoreName];
      Helpers.count({
        datastore: datastore,
        models: models,
        query: query
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          return cb(undefined, report);
        }
      });
    },


    //  ██████╗ ██████╗ ██╗
    //  ██╔══██╗██╔══██╗██║
    //  ██║  ██║██║  ██║██║
    //  ██║  ██║██║  ██║██║
    //  ██████╔╝██████╔╝███████╗
    //  ╚═════╝ ╚═════╝ ╚══════╝
    //
    // Methods related to modifying the underlying data structure of the
    // database.


    //  ╔╦╗╔═╗╔═╗╔═╗╦═╗╦╔╗ ╔═╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐
    //   ║║║╣ ╚═╗║  ╠╦╝║╠╩╗║╣    │ ├─┤├┴┐│  ├┤
    //  ═╩╝╚═╝╚═╝╚═╝╩╚═╩╚═╝╚═╝   ┴ ┴ ┴└─┘┴─┘└─┘
    // Describe a table and get back a normalized model schema format.
    // (This is used to allow Sails to do auto-migrations)
    describe: function describe(datastoreName, tableName, cb, meta) {
      var datastore = datastores[datastoreName];
      Helpers.describe({
        datastore: datastore,
        tableName: tableName,
        meta: meta
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        success: function success(report) {
          // Waterline expects the result to be undefined if the table doesn't
          // exist.
          if (_.keys(report.schema).length) {
            return cb(undefined, report.schema);
          }

          return cb();
        }
      });
    },


    //  ╔╦╗╔═╗╔═╗╦╔╗╔╔═╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐
    //   ║║║╣ ╠╣ ║║║║║╣    │ ├─┤├┴┐│  ├┤
    //  ═╩╝╚═╝╚  ╩╝╚╝╚═╝   ┴ ┴ ┴└─┘┴─┘└─┘
    // Build a new table in the database.
    // (This is used to allow Sails to do auto-migrations)
    define: function define(datastoreName, tableName, definition, cb, meta) {
      var datastore = datastores[datastoreName];
      Helpers.define({
        datastore: datastore,
        tableName: tableName,
        definition: definition,
        meta: meta
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        success: function success() {
          return cb();
        }
      });
    },


    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ┌─┐┌─┐┬ ┬┌─┐┌┬┐┌─┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   └─┐│  ├─┤├┤ │││├─┤
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  └─┘└─┘┴ ┴└─┘┴ ┴┴ ┴
    // Create a new Postgres Schema (namespace) in the database.
    createSchema: function createSchema(datastoreName, schemaName, cb, meta) {
      var datastore = datastores[datastoreName];
      Helpers.createSchema({
        datastore: datastore,
        schemaName: schemaName,
        meta: meta
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        success: function success() {
          return cb();
        }
      });
    },


    //  ╔╦╗╦═╗╔═╗╔═╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐
    //   ║║╠╦╝║ ║╠═╝   │ ├─┤├┴┐│  ├┤
    //  ═╩╝╩╚═╚═╝╩     ┴ ┴ ┴└─┘┴─┘└─┘
    // Remove a table from the database.
    drop: function drop(datastoreName, tableName, relations, cb, meta) {
      var datastore = datastores[datastoreName];
      Helpers.drop({
        datastore: datastore,
        tableName: tableName,
        meta: meta
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        badConnection: function badConnection(err) {
          return cb(err);
        },
        success: function success() {
          return cb();
        }
      });
    },


    //  ╔═╗╔═╗╔╦╗  ┌─┐┌─┐┌─┐ ┬ ┬┌─┐┌┐┌┌─┐┌─┐
    //  ╚═╗║╣  ║   └─┐├┤ │─┼┐│ │├┤ ││││  ├┤
    //  ╚═╝╚═╝ ╩   └─┘└─┘└─┘└└─┘└─┘┘└┘└─┘└─┘
    // Set a sequence in an auto-incrementing primary key to a known value.
    setSequence: function setSequence(datastoreName, sequenceName, sequenceValue, cb, meta) {
      var datastore = datastores[datastoreName];
      Helpers.setSequence({
        datastore: datastore,
        sequenceName: sequenceName,
        sequenceValue: sequenceValue,
        meta: meta
      }).switch({
        error: function error(err) {
          return cb(err);
        },
        success: function success() {
          return cb();
        }
      });
    },

  };

  return adapter;
})();
