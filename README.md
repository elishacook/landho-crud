# landho-crud

Create crud services using [landho](https://github.com/elishacook/landho), [rethinkdb](http://rethinkdb.com/) and [feelings](https://github.com/elishacook/feelings).

There is a companion [client library](https://github.com/elishacook/landho-crud-client) that makes for a nice experience using these services.

[![Build Status][1]][2] [![NPM version][3]][4]

## Install

```bash
npm install landho landho-crud
```

## Quick start

```js
    // Create a landho instance
var api = require('landho')(),
    // Create a CRUD maker that connects to a rethinkdb database
    crud = require('landho-crud')(api, { port: 28015 , db: 'somewhere' })

// Define services using the crud() function
crud(
{
    // This will be the name of the service and the name
    // of the rethinkdb table. landho-crud will create tables
    // on the fly
    name: 'robbers',
    
    // Define a validation schema using the feelings library.
    schema:
    {
        name: { type: String },
        skills: {
            type: Array,
            items: { type: String }
        }
    },
    
    // You can specify indexes as well
    indexes: ['name']
})

// You need to initialize the database before using the services
crud.init().then(function ()
{
    // crud() created a regular landho service
    var robbers = api.service('robbers')
    
    // with a create() method
    robbers.create(
        { data: { name: 'Bob', skills: ['safe cracking', 'doberman dodging'] } },
        function (err, record)
        {
            record.id // 'some-long-id'
            record.name // 'Bob'
            record.skills // ['safe cracking', 'doberman dodging']
            // create() adds modified and created timestamps as well
            record.created // Tue Oct 06 2015 14:55:55 GMT-0400 (EDT)
            record.modified // Tue Oct 06 2015 14:55:55 GMT-0400 (EDT)
            
            // There's also get(), update(), find() and remove() as you might expect.
            
            robbers.get({ data: { id: 'some-long-id' } }, function (err, record)
            {
                // Do stuff with the record
            })
            
            record.name = 'Bob Frankleton'
            
            robbers.update({ data: record }, function (err, updated_record)
            {
                updated_record.name // 'Bob Frankleton'
                updated_record.modified != record.modified // true
            })
            
            robbers.find({}, function (err, all_the_robbers)
            {
                all_the_robbers.forEach(function (robber)
                {
                    if (-1 < robber.skills.indexOf('doberman dodging'))
                    {
                        // We found the 5th member of team heist
                    }
                })
            })
            
            robbers.remove({ data: { id: record.id } }, function (err, deleted_record)
            {
                // Oh no! I got deleted!!
            })
        }
    )
})
```

See the tests for more details.

[1]: https://secure.travis-ci.org/elishacook/landho-crud.svg
[2]: https://travis-ci.org/elishacook/landho-crud
[3]: https://badge.fury.io/js/landho-crud.svg
[4]: https://badge.fury.io/js/landho-crud