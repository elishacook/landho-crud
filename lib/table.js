'use strict'

var db = require('./db'),
    r = require('rethinkdb'),
    Channel = require('landho/lib/channel').Channel,
    SyncDocument = require('sync-document')

module.exports = Table


function Table(name, indexes)
{
    this.name = name
    
    this.table = function ()
    {
        return db.r.table(name)
    }
    
    db.add({ name: name, indexes: indexes })
}


Table.prototype.indexes = function (done)
{
    return this.table().indexList().then(done.bind(null, null)).catch(done)
}


Table.prototype.create = function (doc, done)
{
    return this.table()
        .insert(doc, { returnChanges: true })
        .run()
        .then(function (result)
        {
            return done(null, result.changes[0].new_val)
        })
        .catch(done)
}


Table.prototype.update = function (doc, done)
{
    return this.table()
        .get(doc.id)
        .replace(doc, { returnChanges: true })
        .then(function (result)
        {
            return done(null, result.changes[0].new_val)
        })
        .catch(done)
}


Table.prototype.delete = function (id, done)
{
    var doc
    return this.table()
        .get(id)
        .delete({ returnChanges: true })
        .run()
        .then(function (result)
        {
            done(null, result.changes[0].old_val)
        })
        .catch(done)
}


Table.prototype.get = function (id, done)
{
    return this.table().get(id).run().then(done.bind(null, null)).catch(done)
}


Table.prototype.find = function (options, done)
{
    return this.get_query(options).run().then(done.bind(null, null)).catch(done)
}


Table.prototype.get_and_watch = function (id, done)
{
    return this.table()
        .get(id)
        .changes()
        .run()
        .then(function (cursor)
        {
            var channel = new Channel()
            done(null, channel)
            this.watch_channel(channel, cursor, 'initial')
        }.bind(this))
        .catch(done)
}


Table.prototype.find_and_watch = function (options, done)
{
    return this.get_query(options)
        .run()
        .then(function (results)
        {
            this.get_query(options)
                .changes()
                .run()
                .then(function (cursor)
                {
                    var channel = new Channel()
                    done(null, channel)
                    channel.emit('initial', results)
                    this.watch_channel(channel, cursor)
                }.bind(this))
                .catch(done)
        }.bind(this))
        .catch(done)
}


Table.prototype.watch_channel = function (channel, cursor, insert_name)
{
    insert_name = insert_name || 'insert'
    
    channel.on('close', cursor.close.bind(cursor))
    
    cursor.each(function (err, change)
    {
        if (err)
        {
            channel.emit('error', err)
        }
        else if (change.old_val && change.new_val)
        {
            channel.emit('update', change.new_val)
        }
        else if (change.new_val)
        {
            channel.emit(insert_name, change.new_val)
        }
        else
        {
            channel.emit('delete', change.old_val.id)
        }
    })
}


Table.prototype.get_and_sync = function (id, done)
{
    return this.table()
        .get(id)
        .changes()
        .run()
        .then(function (cursor)
        {
            var channel = new Channel(),
                syncdoc = null
            
            channel.on('close', cursor.close.bind(cursor))
            
            channel.on('pull', function (edits)
            {
                edits.forEach(function (edit)
                {
                    syncdoc.pull(edit)
                })
                
                this.table()
                    .get(syncdoc.object.id)
                    .replace(syncdoc.object)
                    .run()
                    .catch(function (err)
                    {
                        channel.emit('error', err)
                    })
                    
                if (syncdoc.edits.length > 0)
                {
                    channel.emit('pull', syncdoc.edits)
                }
            }.bind(this))
            
            cursor.each(function (err, change)
            {
                if (change.new_val && change.old_val !== undefined)
                {
                    syncdoc.object = change.new_val
                    syncdoc.push()
                    
                    if (syncdoc.edits.length > 0)
                    {
                        channel.emit('pull', syncdoc.edits)
                    }
                }
                else if (change.old_val)
                {
                    channel.emit('delete')
                }
                else if (change.new_val)
                {
                    syncdoc = new SyncDocument(change.new_val)
                    done(null, channel)
                    channel.emit('initial', change.new_val)
                }
            })
        }.bind(this))
        .catch(done)
}


Table.prototype.find_and_sync = function (options, done)
{
    return this.get_query(options)
        .run()
        .then(function (results)
        {
            this.get_query(options)
                .changes()
                .run()
                .then(function (cursor)
                {
                    var channel = new Channel(),
                        syncdocs = {}
                    
                    results.forEach(function (doc)
                    {
                        syncdocs[doc.id] = new SyncDocument(doc)
                    })
                    
                    channel.on('close', cursor.close.bind(cursor))
                    
                    channel.on('pull', function (data)
                    {
                        var syncdoc = syncdocs[data.id]
                        
                        if (!syncdoc)
                        {
                            channel.emit('error', 'Document sync not initialized for '+data.id)
                            return
                        }
                        
                        data.edits.forEach(function (edit)
                        {
                            syncdoc.pull(edit)
                        })
                        
                        this.table()
                            .get(syncdoc.object.id)
                            .replace(syncdoc.object)
                            .run()
                            .catch(function (err)
                            {
                                channel.emit('error', err)
                            })
                        
                        if (syncdoc.edits.length > 0)
                        {
                            channel.emit('pull', { id: data.id, edits: syncdoc.edits })
                        }
                    }.bind(this))
                    
                    done(null, channel)
                    
                    channel.emit('initial', results)
                    
                    cursor.each(function (err, change)
                    {
                        if (change.old_val && change.new_val)
                        {
                            var syncdoc = syncdocs[change.new_val.id]
                            syncdoc.object = change.new_val
                            syncdoc.push()
                            
                            if (syncdoc.edits.length > 0)
                            {
                                channel.emit('pull', { id: change.new_val.id, edits: syncdoc.edits })
                            }
                        }
                        else if (change.new_val)
                        {
                            syncdocs[change.new_val.id] = new SyncDocument(change.new_val)
                            channel.emit('insert', change.new_val)
                        }
                        else if (change.old_val)
                        {
                            channel.emit('delete', change.old_val.id)
                            delete syncdocs[change.old_val.id]
                        }
                    })
                }.bind(this))
                .catch(done)
        }.bind(this))
        .catch(done)
}


Table.prototype.get_query = function (options)
{
    options = options || {}
    
    if (options.start !== undefined || options.end !== undefined)
    {
        return this.get_between_query(options)
    }
    else
    {
        return this.get_find_query(options)
    }
}


Table.prototype.get_find_query = function (options)
{
    var q = this.table()
    
    if (options.value !== undefined && options.index !== undefined)
    {
        var args = null
        if (options.value instanceof Array)
        {
            args = options.value
        }
        else
        {
            args = [options.value]
        }
        
        args.push({ index: options.index })
        
        q = q.getAll.apply(q, args)
    }
    
    if (options.orderBy)
    {
        q = q.orderBy({ index: options.order_by })
    }
    
    if (options.limit)
    {
        q = q.limit(options.limit)
    }
    
    if (options.skip)
    {
        q = q.skip(options.skip)
    }
    
    return q
}


Table.prototype.get_between_query = function (options)
{
    return this.table().between(
        options.start || db.r.minval,
        options.end || db.r.maxval,
        {
            index: options.index,
            leftBound: options.left || 'closed',
            rightBound: options.right || 'open'
        }
    )
}