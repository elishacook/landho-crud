'use strict'

var r = require('rethinkdb'),
    rethinkdbdash = require('rethinkdbdash')

require('rethinkdb-init')(r)

var db =
{
    tables: [],
    
    r: null,
    
    add: function (table_config)
    {
        db.tables.push(table_config)
    },
    
    init: function (options)
    {
        var p = r.init(options, db.tables).then(function (conn)
        {
            db.r = rethinkdbdash(options)
            db.tables = null
        })
        return p
    }
}

module.exports = db