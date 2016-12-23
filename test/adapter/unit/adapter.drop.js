var adapter = require('../../lib/adapter'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Setup('test_drop', done);
  });

  after(function(done) {
    adapter.teardown('test', done);
  });

  /**
   * DROP
   *
   * Drop a table and all it's records.
   */

  describe('.drop()', function() {

    // Drop the Test table
    it('should drop the table', function(done) {

      adapter.drop('test', 'test_drop', function(err, result) {
        adapter.describe('test', 'test_drop', function(err, result) {
          should.not.exist(result);
          done();
        });
      });

    });
  });
});