var Sequel = require('waterline-sequel'),
    should = require('should'),
    Support = require('./support/bootstrap');

describe('query', function() {

  /**
   * groupBy
   *
   * Adds a Group By statement to a sql statement
   */

  describe('.groupBy()', function() {

    describe('with array', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        groupBy: ['name'],
        average: ['age']
      };

      var schema = {'test': Support.Schema('test', { name: { type: 'text' }, age: { type: 'integer'} })};

      it('should append a Group By clause to the select statement', function() {
        var query = new Sequel(schema, Support.SqlOptions).find('test', criteria);
        var sql = 'SELECT "test"."name", CAST( AVG("test"."age") AS float) AS age ' +
                  'FROM "test" AS "test"  WHERE LOWER("test"."name") = $1  GROUP BY "test"."name"';

        query.query[0].should.eql(sql);
      });
    });

    describe('with string', function() {

      // Lookup criteria
      var criteria = {
        where: {
          name: 'foo'
        },
        groupBy: 'name',
        average: 'age'
      };

      var schema = {'test': Support.Schema('test', { name: { type: 'text' }, age: { type: 'integer'} })};

      it('should use the MAX aggregate option in the select statement', function() {
        var query = new Sequel(schema, Support.SqlOptions).find('test', criteria);
        var sql = 'SELECT "test"."name", CAST( AVG("test"."age") AS float) AS age ' +
                  'FROM "test" AS "test"  WHERE LOWER("test"."name") = $1  GROUP BY "test"."name"'

        query.query[0].should.eql(sql);
      });
    });

  });
});