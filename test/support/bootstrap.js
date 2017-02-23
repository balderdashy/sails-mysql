/**
 * Support functions for helping with Postgres tests
 */

var _ = require('@sailshq/lodash');
var MySQL = require('machinepack-mysql');
var adapter = require('../../lib/adapter');

var Support = module.exports = {};

// Determine config (using env vars).
Support.Config = (function(){
  var config = {
    schema: true,
  };

  if (process.env.WATERLINE_ADAPTER_TESTS_URL) {
    config.url = process.env.WATERLINE_ADAPTER_TESTS_URL;
    return config;
  }//‡-•
  else if (process.env.MYSQL_PORT_3306_TCP_ADDR || process.env.WATERLINE_ADAPTER_TESTS_HOST || process.env.WATERLINE_ADAPTER_TESTS_PORT || process.env.MYSQL_ENV_MYSQL_USER || process.env.WATERLINE_ADAPTER_TESTS_USER || process.env.MYSQL_ENV_MYSQL_PASSWORD || process.env.WATERLINE_ADAPTER_TESTS_PASSWORD || process.env.MYSQL_PWD || process.env.MYSQL_ENV_MYSQL_DATABASE || process.env.WATERLINE_ADAPTER_TESTS_DATABASE) {
    console.warn();
    console.warn('Traditional env vars for configuring WAT (waterline adapter tests) are no longer supported.  Instead, please use the `WATERLINE_ADAPTER_TESTS_URL` env var to specify a connection URL (e.g. `WATERLINE_ADAPTER_TESTS_URL=mysql://root@localhost:1337/adapter_tests npm test`).  See **Reference > Configuration > sails.config.datastores** in the Sails docs for more information and examples on using connection URLs.');
    console.warn('(Trying to make it work for you this time...)');
    console.warn();
    config.host = process.env.MYSQL_PORT_3306_TCP_ADDR || process.env.WATERLINE_ADAPTER_TESTS_HOST || 'localhost';
    config.port = process.env.WATERLINE_ADAPTER_TESTS_PORT || 3306;
    config.user = process.env.MYSQL_ENV_MYSQL_USER || process.env.WATERLINE_ADAPTER_TESTS_USER || 'root';
    config.password = process.env.MYSQL_ENV_MYSQL_PASSWORD || process.env.WATERLINE_ADAPTER_TESTS_PASSWORD || process.env.MYSQL_PWD || '';
    config.database = process.env.MYSQL_ENV_MYSQL_DATABASE || process.env.WATERLINE_ADAPTER_TESTS_DATABASE || 'adapter_tests';
    return config;
  }//‡-•
  else {
    throw new Error('Please use the `WATERLINE_ADAPTER_TESTS_URL` env var to specify a connection URL (e.g. `WATERLINE_ADAPTER_TESTS_URL=mysql://root@localhost:1337/adapter_tests npm test`).  See **Reference > Configuration > sails.config.datastores** in the Sails docs for more information and examples on using connection URLs.');
  }

})();

// Fixture Model Def
Support.Model = function model(name, def) {
  return {
    identity: name,
    tableName: name,
    datastore: 'test',
    primaryKey: 'id',
    definition: def || Support.Definition
  };
};

// Fixture Table Definition
Support.Definition = {
  id: {
    type: 'number',
    columnName: 'id',
    autoMigrations: {
      columnType: 'integer',
      autoIncrement: true,
      unique: true
    }
  },
  fieldA: {
    type: 'string',
    columnName: 'fieldA',
    autoMigrations: {
      columnType: 'text'
    }
  },
  fieldB: {
    type: 'string',
    columnName: 'fieldB',
    autoMigrations: {
      columnType: 'text'
    }
  },
  fieldC: {
    type: 'ref',
    columnName: 'fieldC',
    autoMigrations: {
      columnType: 'text'
    }
  }
};

// Register and Define a Collection
Support.Setup = function setup(tableName, cb) {
  var collection = Support.Model(tableName);
  var collections = {};
  collections[tableName] = collection;

  var connection = _.cloneDeep(Support.Config);
  connection.identity = 'test';

  // Setup a primaryKey for migrations
  collection.definition = _.cloneDeep(Support.Definition);

  // Build a schema to represent the underlying physical database structure
  var schema = {};
  _.each(collection.definition, function parseAttribute(attributeVal, attributeName) {
    var columnName = attributeVal.columnName || attributeName;

    // If the attribute doesn't have an `autoMigrations` key on it, ignore it.
    if (!_.has(attributeVal, 'autoMigrations')) {
      return;
    }

    schema[columnName] = attributeVal.autoMigrations;
  });

  // Set Primary Key flag on the primary key attribute
  var primaryKeyAttrName = collection.primaryKey;
  var primaryKey = collection.definition[primaryKeyAttrName];
  if (primaryKey) {
    var pkColumnName = primaryKey.columnName || primaryKeyAttrName;
    schema[pkColumnName].primaryKey = true;
  }


  adapter.registerDatastore(connection, collections, function registerCb(err) {
    if (err) {
      return cb(err);
    }

    adapter.define('test', tableName, schema, cb);
  });
};

// Just register a connection
Support.registerConnection = function registerConnection(tableNames, cb) {
  var collections = {};

  _.each(tableNames, function processTable(name) {
    var collection = Support.Model(name);
    collections[name] = collection;
  });

  var connection = _.cloneDeep(Support.Config);
  connection.identity = 'test';

  adapter.registerDatastore(connection, collections, cb);
};

// Remove a table and destroy the manager
Support.Teardown = function teardown(tableName, cb) {
  var manager = adapter.datastores[_.first(_.keys(adapter.datastores))].manager;
  MySQL.getConnection({
    manager: manager,
    meta: Support.Config
  }).exec(function getConnectionCb(err, report) {
    if (err) {
      return cb(err);
    }

    var query = 'DROP TABLE IF EXISTS `' + tableName + '`;';
    MySQL.sendNativeQuery({
      connection: report.connection,
      nativeQuery: query
    }).exec(function dropTableCb(err) {
      if (err) {
        return cb(err);
      }

      MySQL.releaseConnection({
        connection: report.connection
      }).exec(function releaseConnectionCb(err) {
        if (err) {
          return cb(err);
        }

        delete adapter.datastores[_.first(_.keys(adapter.datastores))];
        return cb();
      });
    });
  });
};

// Seed a record to use for testing
Support.Seed = function seed(tableName, cb) {
  var manager = adapter.datastores[_.first(_.keys(adapter.datastores))].manager;
  MySQL.getConnection({
    manager: manager,
    meta: Support.Config
  }).exec(function getConnectionCb(err, report) {
    if (err) {
      return cb(err);
    }

    var query = [
      'INSERT INTO `' + tableName + '` (`fieldA`, `fieldB`) ',
      'values (\'foo\', \'bar\'), (\'foo_2\', \'bAr_2\');'
    ].join('');

    MySQL.sendNativeQuery({
      connection: report.connection,
      nativeQuery: query
    }).exec(function seedCb(err) {
      if (err) {
        return cb(err);
      }

      MySQL.releaseConnection({
        connection: report.connection
      }).exec(cb);
    });
  });
};
