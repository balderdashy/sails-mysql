/**
 * Run Integration Tests
 *
 * Uses the waterline-adapter-tests module to
 * run mocha tests against the currently implemented
 * waterline API.
 */

var tests = require('waterline-adapter-tests'),
    adapter = require('../../lib/adapter'),
    mocha = require('mocha');

/**
 * Build a MySQL Config File
 */

var config = {
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'sails_mysql'
};

/**
 * Expose Interfaces Used In Adapter
 */

var interfaces = ['semantic', 'queryable', 'migratable', 'associations'];

/**
 * Run Tests
 */

var suite = new tests({ adapter: adapter, config: config, interfaces: interfaces });
