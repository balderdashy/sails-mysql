var adapter = require('../../lib/adapter'),
    _ = require('lodash'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Setup('test_addAttribute', done);
  });

  after(function(done) {
    support.Teardown('test_addAttribute', done);
  });

  /**
   * ADD ATTRIBUTE
   *
   * Adds a column to a Table
   */

  describe('.addAttribute()', function() {

    // Add a column to a table
    it('should add column color to the table', function(done) {

      adapter.addAttribute('test', 'test_addAttribute', 'color', 'string', function(err, result) {
        adapter.describe('test', 'test_addAttribute', function(err, result) {

          // Test Row length
          Object.keys(result).length.should.eql(4);

          // Test the name of the last column
          should.exist(result.color);

          done();
        });
      });

    });
  });
});