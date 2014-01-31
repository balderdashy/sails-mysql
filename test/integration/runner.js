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
  host: process.env.WATERLINE_ADAPTER_TESTS_HOST || 'localhost',
  user: process.env.WATERLINE_ADAPTER_TESTS_USER || 'root',
  password: process.env.WATERLINE_ADAPTER_TESTS_PASSWORD || '',
  database: process.env.WATERLINE_ADAPTER_TESTS_DATABASE || 'sails_mysql',
  pool: true,
  connectionLimit: 10,
  waitForConnections: true
};

/**
 * Expose Interfaces Used In Adapter
 */

var interfaces = ['semantic', 'queryable', 'migratable', 'associations'];

/**
 * Run Tests
 */

var suite = new tests({ adapter: adapter, config: config, interfaces: interfaces });
