'use strict'

var validator = require('./validate'),
    shallow_copy = require('./shallow-copy'),
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
    
    var update_schema = shallow_copy(schema)
    update_schema.id = { type: String, required: true }
    var update_validator = validator(update_schema)
    
    var service = api.service(name,
    {
        create: function (params, done)
        {
            table.create(params.data, params.user ? params.user.id : null).then(done.bind(null, null)).catch(done)
        },
        
        update: function (params, done)
        {
            table.update(params.data, params.user ? params.user.id : null).then(done.bind(null, null)).catch(done)
        },
        
        patch: function (params, done)
        {
            table.patch(
                params.data.id, 
                params.data.version,
                params.data.patch,
                params.user ? params.user.id : null,
                update_validator
            )
            .then(done.bind(null, null)).catch(done)
        },
        
        delete: function (params, done)
        {
            table.delete(params.data.id, params.data.version, params.user ? params.user.id : null).then(done.bind(null, null)).catch(done)
        },
        
        get: function (params)
        {
            return {
                initial: function (done)
                {
                    table.get(params.data.id).then(done.bind(null, null)).catch(done)
                },
                changes: function (subscriber, done)
                {
                    table.watch_one(params.data.id)
                        .then(cursor_adapter(subscriber, done))
                        .catch(done)
                }
            }
        },
        
        find: function (params)
        {
            return {
                initial: function (done)
                {
                    table.find(params.data).then(done.bind(null, null)).catch(done)
                },
                
                changes: function (subscriber, done)
                {
                    table.watch(params.data)
                        .then(cursor_adapter(subscriber, done))
                        .catch(done)
                }
            }
        }
    })
    .before(
    {
        create: validator(schema),
        update: update_validator,
        patch: validator(
        {
            id: { type: String, required: true },
            version: { type: Number, required: true },
            patch: { type: Object, required: true }
        }),
        delete: validator(
        { 
            id: { type: String, required: true }, 
            version: { type: Number, required: true }
        }),
        get: validator({ id: { type: String, required: true } }),
        find: validator(
        {
            index: { type: String },
            value: {},
            limit: { type: Number },
            skip: { type: Number },
            start: {},
            end: {},
            left: { type: String },
            right: { type: String }
        })
    })
    service.table = table
    return service
}

function cursor_adapter (subscriber, done)
{
    return function (cursor)
    {
        cursor.each(function (err, change)
        {
            if (err)
            {
                subscriber.emit('error', { code: 500, message: err.message })
            }
            else
            {
                subscriber.emit('change', change)
            }
        })
        
        done(null, { close: cursor.close.bind(cursor) })
    }
}
