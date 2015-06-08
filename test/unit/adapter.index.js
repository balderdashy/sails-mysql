var adapter = require('../../lib/adapter'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Teardown
   */

  before(function(done) {
    support.registerConnection(['test_index'], done);
  });

  after(function(done) {
    support.Teardown('test_index', done);
  });

  // Attributes for the test table
  var definition = {
    id: {
      type: 'integer',
      autoIncrement: true
    },
    name: {
      type: 'string',
      index: true
    }
  };

  /**
   * Indexes
   *
   * Ensure Indexes get created correctly
   */

  describe('Index Attributes', function() {

    // Build Indicies from definition
    it('should add indicies', function(done) {

      adapter.define('test', 'test_index', definition, function(err) {
        adapter.describe('test', 'test_index', function(err, result) {
          result.name.indexed.should.eql(true);
          done();
        });
      });

    });

  });
});