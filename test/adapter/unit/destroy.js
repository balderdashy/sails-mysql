var assert = require('assert');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Destroy', function() {
    // Test Setup
    before(function(done) {
      Support.Setup('test_destroy', function(err) {
        if (err) {
          return done(err);
        }

        // Seed the database with two simple records.
        Support.Seed('test_destroy', done);
      });
    });

    after(function(done) {
      Support.Teardown('test_destroy', done);
    });

    it('should ensure the record is actually deleted', function(done) {
      var query = {
        using: 'test_destroy',
        criteria: {
          where: {
            fieldA: 'foo_2'
          }
        }
      };

      Adapter.destroy('test', query, function(err) {
        if (err) {
          return done(err);
        }

        Adapter.find('test', query, function(err, results) {
          if (err) {
            return done(err);
          }

          assert.equal(results.length, 0);

          return done();
        });
      });
    });

    // Look into the bowels of the PG Driver and ensure the Create function handles
    // it's connections properly.
    it('should release it\'s connection when completed', function(done) {
      var manager = Adapter.datastores.test.manager;
      var preConnectionsAvailable = manager.pool._allConnections.length;

      var query = {
        using: 'test_destroy',
        criteria: {}
      };

      Adapter.destroy('test', query, function(err) {
        if (err) {
          return done(err);
        }

        var postConnectionsAvailable = manager.pool._allConnections.length;
        assert.equal(preConnectionsAvailable, postConnectionsAvailable);

        return done();
      });
    });
  });
});
