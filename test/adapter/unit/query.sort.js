var Sequel = require('waterline-sequel'), 
    should = require('should'),
    Support = require('./support/bootstrap');

describe('query', function() {

  /**
   * SORT
   *
   * Adds an ORDER BY parameter to a sql statement
   */

  describe('.sort()', function() {

    it('should append the ORDER BY clause to the query', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        sort: {
          name: 1
        }
      };

      var schema = {'test': Support.Schema('test', { name: { type: 'text' } })};

      var query = new Sequel(schema, Support.SqlOptions).find('test', criteria);
      var sql = 'SELECT "test"."name" FROM "test" AS "test"  WHERE LOWER("test"."name") = $1  ' +
                'ORDER BY "test"."name" ASC';

      query.query[0].should.eql(sql);
    });

    it('should sort by multiple columns', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        sort: {
          name: 1,
          age: 1
        }
      };

      var schema = {'test': Support.Schema('test', { name: { type: 'text' }, age: { type: 'integer'} })};

      var query = new Sequel(schema, Support.SqlOptions).find('test', criteria);
      var sql = 'SELECT "test"."name", "test"."age" FROM "test" AS "test"  WHERE LOWER("test"."name") = $1  ' +
                'ORDER BY "test"."name" ASC, "test"."age" ASC';

      query.query[0].should.eql(sql);
    });

    it('should allow desc and asc ordering', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        sort: {
          name: 1,
          age: -1
        }
      };

      var schema = {'test': Support.Schema('test', { name: { type: 'text' }, age: { type: 'integer'} })};

      var query = new Sequel(schema, Support.SqlOptions).find('test', criteria);
      var sql = 'SELECT "test"."name", "test"."age" FROM "test" AS "test"  WHERE LOWER("test"."name") = $1  ' +
                'ORDER BY "test"."name" ASC, "test"."age" DESC';

      query.query[0].should.eql(sql);
    });

  });
});