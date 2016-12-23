var Sequel = require('waterline-sequel'),
    should = require('should'),
    Support = require('./support/bootstrap');

describe('query', function() {

  /**
   * SKIP
   *
   * Adds an OFFSET parameter to a sql statement
   */

  describe('.skip()', function() {

    // Lookup criteria
    var criteria = {
      where: {
        name: 'foo'
      },
      skip: 1
    };

    var schema = {'test': Support.Schema('test', { name: { type: 'text' } })};

    it('should append the SKIP clause to the query', function() {
      var query = new Sequel(schema, Support.SqlOptions).find('test', criteria);
      var sql = 'SELECT "test"."name" FROM "test" AS "test"  WHERE LOWER("test"."name") = $1  LIMIT 184467440737095516  OFFSET 1';
      query.query[0].should.eql(sql);
    });

  });
});