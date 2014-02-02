![image_squidhome@2x.png](http://i.imgur.com/RIvu9.png) 

# MySQLAdapter

Adds MySQL support for Sails.

# Sails.js Repo
http://SailsJs.org


## About Waterline
Waterline is a new kind of storage and retrieval engine.  It provides a uniform API for accessing stuff from different kinds of databases, protocols, and 3rd party APIs.  That means you write the same code to get users, whether they live in mySQL, LDAP, MongoDB, or Facebook.
Waterline also comes with built-in transaction support, as well as a configurable environment setting. 


## Writing your own adapters
It's easy to add your own adapters for integrating with proprietary systems or existing open APIs.  For most things, it's as easy as `require('some-module')` and mapping the appropriate methods to match waterline semantics.

## Installation

Install from NPM.

```bash
$ npm install sails-mysql
```

## Sails Configuration

Add the mysql config to the config/adapters.js file.  Basic options:

```javascript
module.exports.adapters = {
  'default': 'mysql',
host: config.host,
      port: config.port || 3306,
      socketPath: config.socketPath || null,
      user: config.user,
      password: config.password,
      database: config.database,
      timezone: config.timezone || 'Z'

  mysql: {
    module   : 'sails-mysql',
    host     : 'localhost',
    port     : 3306,
    user     : 'username',
    password : 'password',
    database : 'MySQL Database Name',
    
    // OR (exlicit sets take precedence)
    module   : 'sails-mongo',
    url      : 'mysql2://USER:PASSWORD@HOST:PORT/DATABASENAME',
    }
  }

 
};
```
[![githalytics.com alpha](https://cruel-carlota.pagodabox.com/a22d3919de208c90c898986619efaa85 "githalytics.com")](http://githalytics.com/mikermcneil/sails-mysql)
