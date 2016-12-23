var adapter = require('../../lib/adapter'),
    should = require('should'),
    support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Setup('test_create', done);
  });

  after(function(done) {
    support.Teardown('test_create', done);
  });

  // Attributes for the test table
  var attributes = {
    field_1: 'foo',
    field_2: 'bar'
  };

  /**
   * CREATE
   *
   * Insert a row into a table
   */

  describe('.create()', function() {

    // Insert a record
    it('should insert a single record', function(done) {
      adapter.create('test', 'test_create', attributes, function(err, result) {

        // Check record was actually inserted
        support.Client(function(err, client, close) {
          client.query('SELECT * FROM test_create', function(err, rows) {
            if (err) {
              return done(err);
            }
            // Test 1 row is returned
            rows.length.should.eql(1);

            // close client
            client.end();

            done();
          });
        });
      });
    });

    // Create Auto-Incremented ID
    it('should create an auto-incremented ID field', function(done) {
      adapter.create('test', 'test_create', attributes, function(err, result) {

        // Should have an ID of 2
        result.id.should.eql(2);

        done();
      });
    });

    it('should keep case', function(done) {
      var attributes = {
        field_1: 'Foo',
        field_2: 'bAr'
      };

      adapter.create('test', 'test_create', attributes, function(err, result) {

        result.field_1.should.eql('Foo');
        result.field_2.should.eql('bAr');

        done();
      });
    });

  });
});