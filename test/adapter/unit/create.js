var assert = require('assert');
var _ = require('@sailshq/lodash');
var Adapter = require('../../../lib/adapter');
var Support = require('../../support/bootstrap');

describe('Unit Tests ::', function() {
  describe('Create', function() {
    // Test Setup
    before(function(done) {
      Support.Setup('test_create', done);
    });

    after(function(done) {
      Support.Teardown('test_create', done);
    });

    // Attributes for the test table
    var attributes = {
      fieldA: 'foo',
      fieldB: 'bar'
    };

    it('should insert a record into the database and return it\'s fields', function(done) {
      var query = {
        using: 'test_create',
        newRecord: attributes,
        meta: {
          fetch: true
        }
      };

      Adapter.create('test', query, function(err, result) {
        if (err) {
          return done(err);
        }

        assert(_.isObject(result));
        assert(!_.isFunction(result));
        assert(!_.isArray(result));
        assert.equal(result.fieldA, 'foo');
        assert.equal(result.fieldB, 'bar');
        assert(result.id);

        return done();
      });
    });

    // Create Auto-Incremented ID
    it('should create an auto-incremented id field', function(done) {
      var query = {
        using: 'test_create',
        newRecord: attributes,
        meta: {
          fetch: true
        }
      };

      Adapter.create('test', query, function(err, result) {
        if (err) {
          return done(err);
        }

        assert(_.isObject(result));
        assert(!_.isFunction(result));
        assert(!_.isArray(result));
        assert(result.id);

        return done();
      });
    });

    it('should keep case', function(done) {
      var query = {
        using: 'test_create',
        newRecord: {
          fieldA: 'Foo',
          fieldB: 'bAr'
        },
        meta: {
          fetch: true
        }
      };

      Adapter.create('test', query, function(err, result) {
        if (err) {
          return done(err);
        }

        assert.equal(result.fieldA, 'Foo');
        assert.equal(result.fieldB, 'bAr');

        return done();
      });
    });

    it('should error for type ref on non buffers', function(done) {
      var query = {
        using: 'test_create',
        newRecord: {
          fieldA: 'Foo',
          fieldB: 'bAr',
          fieldC: 'baz'
        },
        meta: {
          fetch: true
        }
      };

      Adapter.create('test', query, function(err) {
        assert(err);
        return done();
      });
    });

    // Look into the bowels of the PG Driver and ensure the Create function handles
    // it's connections properly.
    it('should release it\'s connection when completed', function(done) {
      var manager = Adapter.datastores.test.manager;
      var preConnectionsAvailable = manager.pool._allConnections.length;

      var query = {
        using: 'test_create',
        newRecord: attributes,
        meta: {
          fetch: true
        }
      };

      Adapter.create('test', query, function(err) {
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
