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
    bail: false
  },

  // Load the adapter module.
  adapter: Adapter,

  // Default connection config to use.
  config: {
    host: process.env.POSTGRES_1_PORT_5432_TCP_ADDR || process.env.WATERLINE_ADAPTER_TESTS_HOST || 'localhost',
    user: process.env.POSTGRES_ENV_POSTGRES_USER || process.env.WATERLINE_ADAPTER_TESTS_USER || 'sails',
    password: process.env.POSTGRES_ENV_POSTGRES_PASSWORD || process.env.WATERLINE_ADAPTER_TESTS_PASSWORD || 'sails',
    database: process.env.POSTGRES_ENV_POSTGRES_DB || process.env.WATERLINE_ADAPTER_TESTS_DATABASE || 'sailspg',
    port: process.env.POSTGRES_PORT_5432_TCP_PORT || process.env.WATERLINE_ADAPTER_TESTS_PORT || 5432,
    schema: true
  },

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
