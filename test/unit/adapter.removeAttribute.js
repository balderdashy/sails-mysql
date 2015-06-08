var adapter = require('../../lib/adapter'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Setup('test_removeAttribute', done);
  });

  after(function(done) {
    support.Teardown('test_removeAttribute', done);
  });

  /**
   * REMOVE ATTRIBUTE
   *
   * Drops a Column from a Table
   */

  describe('.removeAttribute()', function() {

    // Remove a column to a table
    it('should remove column field_2 from the table', function(done) {

      adapter.removeAttribute('test', 'test_removeAttribute', 'field_2', function(err) {
        adapter.describe('test', 'test_removeAttribute', function(err, result) {

          // Test Row length
          Object.keys(result).length.should.eql(2);

          // Test the name of the last column
          should.not.exist(result.field_2);

          done();
        });
      });

    });
  });
});