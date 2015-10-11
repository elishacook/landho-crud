'use strict'

var db = require('./db'),
    r = require('rethinkdb'),
    jiff = require('jiff')

module.exports = Table

function Table(name, indexes)
{
    this.table = function ()
    {
        return db.r.table(name)
    }
    
    this.op_table = function ()
    {
        return db.r.table(name+'_ops')
    }
    
    indexes = indexes || []
    indexes.push({ name: 'id_version', indexFunction: [r.row('id'), r.row('version')] })
    db.add({ name: name, indexes: indexes })
    db.add({
        name: name+'_ops',
        indexes: [
            'docid',
            {
                name: 'docid_version', 
                indexFunction: [r.row('docid'), r.row('version')]
            }
        ]
    })
}


Table.prototype.indexes = function ()
{
    return this.table().indexList().then(function (indexes)
    {
        indexes.splice(indexes.indexOf('id_version'), 1)
        return indexes
    })
}


Table.prototype.create = function (doc, user_id)
{
    doc.version = 1
    doc.created = doc.modified = new Date()
    return this.table()
        .insert(doc, { returnChanges: true })
        .run()
        .then(function (result)
        {
            var new_doc = result.changes[0].new_val
            return this.op_table().insert({
                docid: new_doc.id,
                version: 0,
                when: new_doc.modified,
                user_id: user_id,
                created: 1
            })
            .run()
            .then(function ()
            {
                return new_doc
            })
        }.bind(this))
}


Table.prototype.update = function (doc, user_id)
{
    return this.table().get(doc.id).then(function (old_doc)
    {
        var patch = get_patch_without_auto_fields(old_doc, doc)
        return this.patch(old_doc.id, old_doc.version, patch, user_id)
    }.bind(this))
}


Table.prototype.patch = function (id, version, patch, user_id, validate)
{
    return this.table().get(id)
        .then(function (doc)
        {
            if (doc.version == version)
            {
                var new_doc = jiff.patch(patch, doc)
                
                if (validate)
                {
                    validate(new_doc)
                }
                
                new_doc.version += 1
                new_doc.modified = new Date()
                
                return this.table()
                    .getAll([new_doc.id, version], { index: 'id_version' })
                    .replace(new_doc)
                    .run()
                    .then(function (result)
                    {
                        if (result.skipped == 1)
                        {
                            return this.conflict(new_doc.id, version)
                        }
                        else
                        {
                            return this.op_table()
                                .insert(
                                {
                                    docid: new_doc.id,
                                    version: version,
                                    when: new_doc.modified,
                                    user_id: user_id,
                                    patch: patch
                                })
                                .then(function ()
                                {
                                    return {
                                        document: new_doc
                                    }
                                })
                        }
                    }.bind(this))
            }
            else
            {
                if (version < doc.version)
                {
                    return this.conflict(doc.id, version)
                }
                else
                {
                    throw new Error('Invalid version number')
                }
            }
        }.bind(this))
}


Table.prototype.conflict = function (id, version)
{
    return this.op_table()
        .between([id, version], [id, db.r.maxval], { index: 'docid_version' })
        .run()
        .then(function (ops)
        {
            return {
                conflict: true,
                ops: ops.sort(function (a, b)
                {
                    return a.version - b.version
                })
            }
        })
}


Table.prototype.delete = function (id, version, user_id)
{
    return this.table()
        .getAll([id, version], { index: 'id_version' })
        .delete({ returnChanges: true })
        .run()
        .then(function (result)
        {
            if (result.deleted == 1)
            {
                var doc = result.changes[0].old_val
                
                return this.op_table()
                    .insert({
                        docid: doc.id,
                        version: doc.version,
                        when: new Date(),
                        user_id: user_id,
                        deleted: 1
                    })
                    .then(function (result)
                    {
                        return { document: doc }
                    })
            }
            else
            {
                return this.conflict(id, version)
            }
        }.bind(this))
}


Table.prototype.get = function (id)
{
    return this.table().get(id).run()
}


Table.prototype.watch_one = function (id)
{
    return this.op_table()
        .getAll(id, { index: 'docid' })
        .changes()
        .run()
        .then(function (cursor)
        {
            return {
                each: function (fn)
                {
                    cursor.each(function (err, change)
                    {
                        if (err)
                        {
                            fn(err)
                        }
                        else if (change.new_val && !change.old_val)
                        {
                            fn(null, change.new_val)
                        }
                    })
                },
                
                close: cursor.close.bind(cursor)
            }
        })
}


Table.prototype.find = function (options)
{
    return this.get_query(options).run()
}


Table.prototype.watch = function (options)
{
    return this.get_query(options)
        .changes()
        .run()
        .then(function (cursor)
        {
            return {
                each: function (fn)
                {
                    cursor.each(function (err, change)
                    {
                        if (err)
                        {
                            fn(err)
                        }
                        else if (change.new_val && change.old_val)
                        {
                            var patch = get_patch_without_auto_fields(change.old_val, change.new_val)
                            
                            fn(
                                null,
                                {
                                    docid: change.old_val.id,
                                    version: change.old_val.version,
                                    when: change.new_val.modified,
                                    patch: patch
                                }
                            )
                        }
                        else if (change.new_val)
                        {
                            fn(
                                null,
                                {
                                    docid: change.new_val.id,
                                    version: 0,
                                    when: change.new_val.created,
                                    created: 1
                                }
                            )
                        }
                        else
                        {
                            fn(
                                null,
                                {
                                    docid: change.old_val.id,
                                    version: change.old_val.version,
                                    when: new Date(),
                                    deleted: 1
                                }
                            )
                        }
                    })
                },
                close: cursor.close.bind(cursor)
            }
        })
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

function get_patch_without_auto_fields (a, b)
{
    return jiff.diff(a, b)
            .filter(function (op)
            {
                return op.path != '/version' && op.path != '/modified' && op.path != '/created'
            })
}