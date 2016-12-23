var Sequel = require('waterline-sequel'),
    _ = require('lodash'),
    should = require('should'),
    Support = require('./support/bootstrap');

describe('query', function() {

  /**
   * Joins
   *
   * Build up SQL queries using joins and subqueries.
   */

  describe('.joins()', function() {

    var petSchema = {
      name: 'string',
      id: {
        type: 'integer',
        autoIncrement: true,
        primaryKey: true,
        unique: true
      },
      createdAt: { type: 'datetime', default: 'NOW' },
      updatedAt: { type: 'datetime', default: 'NOW' },
      owner: {
        columnName: 'owner_id',
        type: 'integer',
        foreignKey: true,
        references: 'user',
        on: 'id',
        onKey: 'id'
      }
    };

    var userSchema = {
      name: 'string',
      id: {
        type: 'integer',
        autoIncrement: true,
        primaryKey: true,
        unique: true
      },
      createdAt: { type: 'datetime', default: 'NOW' },
      updatedAt: { type: 'datetime', default: 'NOW' },
      pets: {
        collection: 'pet',
        via: 'user',
        references: 'pet',
        on: 'owner_id',
        onKey: 'user'
      }
    };

    // Simple populate criteria, ex: .populate('pets')
    describe('populates', function() {

      // Lookup criteria
      var criteria =  {
        instructions: {
          pet: {
            strategy: {strategy: 1, meta: { parentFK: 'id' }},
            instructions: [
             { parent: 'user',
               parentKey: 'id',
               child: 'pet',
               childKey: 'owner',
               select: [ 'name', 'id', 'createdAt', 'updatedAt', 'owner' ],
               alias: 'pet',
               removeParentKey: true,
               model: true,
               collection: false,
               criteria: {}
              }
            ]
          }
        },
        where: null,
        limit: 30,
        skip: 0
      };

      var schemaDef = {'user': Support.Schema('user', userSchema), 'pet': Support.Schema('pet', petSchema)};

      it('should build a query using inner joins', function() {
        var query = new Sequel(schemaDef, Support.SqlOptions).find('user', criteria);
        var sql = 'SELECT "user"."name", "user"."id", "user"."createdAt", "user"."updatedAt", '+
                  '"__pet"."name" AS "id___name", "__pet"."id" AS "id___id", "__pet"."createdAt" ' +
                  'AS "id___createdAt", "__pet"."updatedAt" AS "id___updatedAt", "__pet"."owner_id" ' + 
                  'AS "id___owner_id" FROM "user" AS "user"  LEFT OUTER JOIN "pet" AS "__pet" ON ' +
                  '"user".\"id\" = \"__pet\".\"owner\"  LIMIT 30 OFFSET 0';
        query.query[0].should.eql(sql);
      });
    });

  });
});