/**
 * Run integration tests
 *
 * Uses the `waterline-adapter-tests` module to
 * run mocha tests against the appropriate version
 * of Waterline.  Only the interfaces explicitly
 * declared in this adapter's `package.json` file
 * are tested. (e.g. `queryable`, `semantic`, etc.)
 */


/**
 * Module dependencies
 */

var util = require('util');
var TestRunner = require('waterline-adapter-tests');
var Adapter = require('../../../lib/adapter');


// Grab targeted interfaces from this adapter's `package.json` file:
var package = {};
var interfaces = [];
var features = [];

try {
  package = require('../../../package.json');
  interfaces = package.waterlineAdapter.interfaces;
  features = package.waterlineAdapter.features;
} catch (e) {
  throw new Error(
    '\n' +
    'Could not read supported interfaces from `waterlineAdapter.interfaces`' + '\n' +
    'in this adapter\'s `package.json` file ::' + '\n' +
    util.inspect(e)
  );
}


console.log('Testing `' + package.name + '`, a Sails/Waterline adapter.');
console.log('Running `waterline-adapter-tests` against ' + interfaces.length + ' interfaces...');
console.log('( ' + interfaces.join(', ') + ' )');
console.log();
console.log('Latest draft of Waterline adapter interface spec:');
console.log('http://links.sailsjs.org/docs/plugins/adapters/interfaces');
console.log();


// //////////////////////////////////////////////////////////////////////
// Integration Test Runner
//
// Uses the `waterline-adapter-tests` module to
// run mocha tests against the specified interfaces
// of the currently-implemented Waterline adapter API.
// //////////////////////////////////////////////////////////////////////
new TestRunner({

  // Mocha opts
  mocha: {
    bail: false,
    timeout: 20000
  },

  // Load the adapter module.
  adapter: Adapter,

  // Default connection config to use.
  config: (function(){
    var config = {
      schema: true,
    };

    // Try and build up a Waterline Adapter Tests URL if one isn't set.
    // (Not all automated test runners can be configured to automatically set these).
    // Docker sets various URL's that can be used to build up a URL for instance.
    if (process.env.WATERLINE_ADAPTER_TESTS_URL) {
      config.url = process.env.WATERLINE_ADAPTER_TESTS_URL;
      return config;
    }
    else {
      var host = process.env.MYSQL_PORT_3306_TCP_ADDR || process.env.WATERLINE_ADAPTER_TESTS_HOST || 'localhost';
      var port = process.env.WATERLINE_ADAPTER_TESTS_PORT || 3306;
      var user = process.env.MYSQL_ENV_MYSQL_USER || process.env.WATERLINE_ADAPTER_TESTS_USER || 'root';
      var password = process.env.MYSQL_ENV_MYSQL_PASSWORD || process.env.WATERLINE_ADAPTER_TESTS_PASSWORD || process.env.MYSQL_PWD || '';
      var database = process.env.MYSQL_ENV_MYSQL_DATABASE || process.env.WATERLINE_ADAPTER_TESTS_DATABASE || 'adapter_tests';

      config.url = 'mysql://' + user + ':' + password + '@' + host + ':' + port + '/' + database;
      return config;
    }

  })(),

  failOnError: true,
  // The set of adapter interfaces to test against.
  // (grabbed these from this adapter's package.json file above)
  interfaces: interfaces,

  // The set of adapter features to test against.
  // (grabbed these from this adapter's package.json file above)
  features: features,

  // Most databases implement 'semantic' and 'queryable'.
  //
  // As of Sails/Waterline v0.10, the 'associations' interface
  // is also available.  If you don't implement 'associations',
  // it will be polyfilled for you by Waterline core.  The core
  // implementation will always be used for cross-adapter / cross-connection
  // joins.
  //
  // In future versions of Sails/Waterline, 'queryable' may be also
  // be polyfilled by core.
  //
  // These polyfilled implementations can usually be further optimized at the
  // adapter level, since most databases provide optimizations for internal
  // operations.
  //
  // Full interface reference:
  // https://github.com/balderdashy/sails-docs/blob/master/adapter-specification.md
});
