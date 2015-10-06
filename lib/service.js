'use strict'

var db = require('./db'),
    validator = require('./validate')

var timestamp_hooks = 
{
    create: function (params, next)
    {
        params.data.created = params.data.modified = new Date()
        next()
    },
    
    update: function (params, next)
    {
        params.data.modified = new Date()
        next()
    }
}

var validators = function (schema)
{
    schema.id = { type: String }
    
    var update_schema = JSON.parse(JSON.stringify(schema))
    update_schema.id.required = true
    
    var get_schema = { id: { type: String } }
        
    return {
        create: validator(schema),
        update: validator(update_schema),
        get: validator(get_schema),
        remove: validator(get_schema)
    }
}

var create_service = function (name)
{
    var make_change_done = function (done)
    {
        return function (err, res)
        {
            if (err)
            {
                done(err)
            }
            else
            {
                var change = res.changes[0]
                
                if (change.new_val)
                {
                    done(null, change.new_val)
                }
                else
                {
                    done(null, change.old_val)
                }
            }
        }
    }
    
    return {
        
        create: function (params, done)
        {
            var change_done = make_change_done(done)
            db.r.table(name).insert(params.data, { returnChanges: true }).run().then(change_done.bind(null, null), change_done)
        },
        
        update: function (params, done)
        {
            var change_done = make_change_done(done)
            db.r.table(name).replace(params.data, { returnChanges: true }).run().then(change_done.bind(null, null), change_done)
        },
        
        remove: function (params, done)
        {
            var change_done = make_change_done(done, 'old_val')
            db.r.table(name).get(params.data.id).delete({ returnChanges: true }).run().then(change_done.bind(null, null), change_done)
        },
        
        get: function (params)
        {
            return {
                initial: function (done)
                {
                    db.r.table(name).get(params.data.id).run().then(done.bind(null, null), done)
                },
                changes: function (subscriber, done)
                {
                    db.r.table(name).get(params.data.id).changes().run().then(
                        function (cursor)
                        {
                            cursor.each(function (err, change)
                            {
                                if (err)
                                {
                                    subscriber.emit('error', { code: 500, message: err.message })
                                }
                                else if (change.old_val && change.new_val)
                                {
                                    subscriber.emit('update', change.new_val)
                                }
                                else if (change.old_val)
                                {
                                    subscriber.emit('remove', change.old_val)
                                }
                            })
                            
                            done(null, { close: cursor.close.bind(cursor) })
                        },
                        done
                    )
                }
            }
        },
        
        find: function (params)
        {
            var q = function ()
            {
                var q = db.r.table(name)
                
                if (params.data)
                {
                    ['filter', 'orderBy', 'skip', 'limit'].forEach(function (k)
                    {
                        if (params.data[k])
                        {
                            q = q[k](params.data[k])
                        }
                    })
                }
                
                return q
            }
            
            return {
                initial: function (done)
                {
                    q().run().then(done.bind(null, null), done)
                },
                
                changes: function (subscriber, done)
                {
                    q().changes().run().then(
                        function (cursor)
                        {
                            cursor.each(function (err, change)
                            {
                                if (err)
                                {
                                    subscriber.emit('error', { code: 500, message: err.message })
                                }
                                else if (change.old_val && change.new_val)
                                {
                                    subscriber.emit('update', change.new_val)
                                }
                                else if (change.new_val)
                                {
                                    subscriber.emit('append', change.new_val)
                                }
                                else
                                {
                                    subscriber.emit('remove', change.old_val)
                                }
                            })
                            
                            done(null, { close: cursor.close.bind(cursor) })
                        },
                        done
                    )
                }
            }
        }
    }
}

module.exports = function (api, options)
{
    var name = options.name,
        table_name = name.replace(/[\.\/\-]/g, '_'),
        schema = options.schema,
        indexes = options.indexes || []
    
    schema.created = { type: Date, required: true }
    schema.modified = { type: Date, required: true }
    
    db.add({
        name: table_name,
        indexes: indexes
    })
    
    var service = api.service(name, create_service(table_name))
    service.before(timestamp_hooks)
    service.before(validators(schema))
    
    return service
}