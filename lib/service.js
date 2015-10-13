'use strict'

var validator = require('./validate'),
    copy = require('./shallow-copy'),
    Table = require('./table')

module.exports = function (api, options)
{
    var name = options.name,
        table_name = name.replace(/[\.\/\-]/g, '_'),
        schema = options.schema,
        indexes = options.indexes || [],
        table_cls = options.table_cls || Table
    
    var table = new table_cls(name, indexes)
    
    if (!schema.id)
    {
        schema.id = { type: String }
    }
    schema.created = { type: Date }
    schema.modified = { type: Date }
    
    var update_schema = copy(schema)
    update_schema.id = { type: String, required: true }
    var update_validator = validator(update_schema)
    
    var service = api.service(name,
    {
        create: function (params, done)
        {
            table.create(params.data, done)
        },
        
        update: function (params, done)
        {
            table.update(params.data, done)
        },
        
        delete: function (params, done)
        {
            table.delete(params.data.id, done)
        },
        
        get: function (params, done)
        {
            if (params.socket)
            {
                if (params.data.sync)
                {
                    table.get_and_sync(params.data.id, done)
                    return
                }
                else if (params.data.watch)
                {
                    table.get_and_watch(params.data.id, done)
                    return
                }
            }
            
            table.get(params.data.id, done)
        },
        
        find: function (params, done)
        {
            if (params.socket)
            {
                if (params.data.sync)
                {
                    table.find_and_sync(params.data, done)
                    return
                }
                else if (params.data.watch)
                {
                    table.find_and_watch(params.data, done)
                    return
                }
            }
            
            table.find(params.data, done)
        },
        
        indexes: function (params, done)
        {
            table.indexes(done)
        }
    })
    .before(
    {
        create: validator(schema),
        update: update_validator,
        delete: validator(
        { 
            id: { type: String, required: true }
        }),
        get: validator(
        {
            id: { type: String, required: true },
            watch: { type: Boolean },
            sync: { type: Boolean }
        }),
        find: validator(
        {
            index: { type: String },
            value: {},
            limit: { type: Number },
            skip: { type: Number },
            start: {},
            end: {},
            left: { type: String },
            right: { type: String },
            watch: { type: Boolean },
            sync: { type: Boolean }
        })
    })
    
    service.table = table
    
    return service
}
