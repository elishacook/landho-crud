'use strict'

var r = require('rethinkdb'),
    db = require('../lib/db'),
    Table = require('../lib/table'),
    Promise = require('bluebird'),
    db_options = {
        host: 'localhost',
        port: 28015,
        db: 'landho_crud_test'
    },
    jiff = require('jiff'),
    jiff_rebase = require('jiff/lib/rebase'),
    shallow_copy = require('../lib/shallow-copy'),
    monsters = null


function setup()
{
    monsters = new Table('monsters', [ 'height', 'scariness' ])
    return db.init(db_options)
}

function get_db()
{
    return db.r.db(db_options.db)
}

function clear_tables()
{
    return get_db().tableList().run()
        .then(function (tables)
        {
            return Promise.all(tables.map(function (t)
            {
                return get_db().table(t).delete().run()
            }))
        })
}

describe('Table', function ()
{
    before(setup)
    beforeEach(clear_tables)
    
    it('adds tables to db', function ()
    {
        db.tables = []
        var foos = new Table('foos', ['things', 'stuff'])
        
        expect(db.tables.length).to.equal(2)
        
        expect(db.tables[0].name).to.equal('foos')
        expect(db.tables[0].indexes.length).to.equal(3)
        expect(db.tables[0].indexes[0]).to.equal('things')
        expect(db.tables[0].indexes[1]).to.equal('stuff')
        expect(db.tables[0].indexes[2].name).to.equal('id_version')
        expect(db.tables[0].indexes[2].indexFunction[0].args[1].data).to.equal('id')
        expect(db.tables[0].indexes[2].indexFunction[1].args[1].data).to.equal('version')
        
        expect(db.tables[1].name).to.equal('foos_ops')
        expect(db.tables[1].indexes.length).to.equal(2)
        expect(db.tables[1].indexes[0]).to.equal('docid')
        expect(db.tables[1].indexes[1].name).to.equal('docid_version')
        expect(db.tables[1].indexes[1].indexFunction[0].args[1].data).to.equal('docid')
        expect(db.tables[1].indexes[1].indexFunction[1].args[1].data).to.equal('version')
    })
    
    it('can create a new record', function ()
    {
        return monsters
            .create({ name: 'werewolf', height: 1.2, scariness: 7.3256 }, '123')
            .then(function (doc)
            {
                expect(doc.id).to.not.be.undefined
                expect(doc.name).to.equal('werewolf')
                expect(doc.height).to.equal(1.2)
                expect(doc.scariness).to.equal(7.3256)
                
                return get_db().table('monsters_ops')
                        .getAll(doc.id, {index:'docid'})
                        .run()
                        .then(function (ops)
                        {
                            expect(ops.length).to.equal(1)
                            expect(ops[0].docid).to.equal(doc.id)
                            expect(ops[0].when).to.be.instanceof(Date)
                            expect(ops[0].user_id).to.equal('123')
                            expect(ops[0].created).to.equal(1)
                        })
            })    
    })
    
    it('can update a record', function ()
    {
        var orig_doc = null
            
        return monsters
                .create({ name: 'werewolf', height: 1.2, scariness: 7.3256 }, '123')
                .then(function (doc)
                {
                    orig_doc = doc
                    var updated_doc = shallow_copy(doc)
                    updated_doc.height = 1.7
                    return monsters.update(updated_doc, '456')
                })
                .then(function (patched_doc)
                {
                    expect(patched_doc.id).to.equal(orig_doc.id)
                    expect(patched_doc.version).to.equal(orig_doc.version + 1)
                    expect(patched_doc.height).to.equal(1.7)
                    
                    return get_db().table('monsters_ops')
                            .getAll(patched_doc.id, { index: 'docid' })
                            .run()
                })
                .then(function (ops)
                {
                    ops = ops.sort(function (a, b)
                    {
                        return a.version - b.version
                    })
                    
                    expect(ops.length).to.equal(2)
                    expect(ops[0].created).to.equal(1)
                    expect(ops[1].docid).to.equal(orig_doc.id)
                    expect(ops[1].version).to.equal(1)
                    expect(ops[1].when).to.be.instanceof(Date)
                    expect(ops[1].user_id).to.equal('456')
                })
    })
    
    it('can patch a record', function ()
    {
        var orig_doc = null,
            patch = null
            
        return monsters
                .create({ name: 'werewolf', height: 1.2, scariness: 7.3256 }, '123')
                .then(function (doc)
                {
                    orig_doc = doc
                    var new_doc = shallow_copy(doc)
                    new_doc.height = 1.7
                    patch = jiff.diff(doc, new_doc)
                    return monsters.patch(doc.id, doc.version, patch, '456')
                })
                .then(function (patched_doc)
                {
                    expect(patched_doc.id).to.equal(orig_doc.id)
                    expect(patched_doc.version).to.equal(orig_doc.version + 1)
                    expect(patched_doc.height).to.equal(1.7)
                    
                    return get_db().table('monsters_ops')
                            .getAll(patched_doc.id, { index: 'docid' })
                            .run()
                })
                .then(function (ops)
                {
                    ops = ops.sort(function (a, b)
                    {
                        return a.version - b.version
                    })
                    
                    expect(ops.length).to.equal(2)
                    expect(ops[0].created).to.equal(1)
                    expect(ops[1].docid).to.equal(orig_doc.id)
                    expect(ops[1].version).to.equal(1)
                    expect(ops[1].when).to.be.instanceof(Date)
                    expect(ops[1].user_id).to.equal('456')
                    expect(ops[1].patch).to.deep.equal(patch)
                })
    })
    
    it('can rebase a patch', function ()
    {
        var orig_doc = null,
            patch_a = null,
            patch_b = null
        
        return monsters
            .create({ name: 'werewolf', height: 1.2, scariness: 7.3256 }, '123')
            .then(function (doc)
            {
                orig_doc = doc
                var update_a = shallow_copy(orig_doc)
                update_a.scariness = 0.2
                patch_a = jiff.diff(orig_doc, update_a)
                return monsters.patch(orig_doc.id, orig_doc.version, patch_a, '444')
            })
            .then(function (doc)
            {
                var update_b = shallow_copy(orig_doc)
                update_b.height = 0.02
                patch_b = jiff.diff(orig_doc, update_b)
                return monsters.patch(orig_doc.id, orig_doc.version, patch_b, '555')
            })
            .then(function (updated_doc)
            {
                expect(updated_doc.id).to.equal(orig_doc.id)
                expect(updated_doc.version).to.equal(3)
                expect(updated_doc.scariness).to.equal(0.2)
                expect(updated_doc.height).to.equal(0.02)
                
                return get_db()
                    .table('monsters_ops')
                    .between([orig_doc.id, 1], [orig_doc.id, db.r.maxval], { index: 'docid_version' })
                    .then(function (ops)
                    {
                        ops = ops.sort(function (a, b)
                        {
                            return a.version - b.version
                        })
                        expect(ops[0].version).to.equal(1)
                        expect(ops[0].user_id).to.equal('444')
                        expect(ops[0].patch).to.deep.equal(patch_a)
                        
                        var patch_c = jiff_rebase([patch_a], patch_b)
                        
                        expect(ops[1].version).to.equal(2)
                        expect(ops[1].user_id).to.equal('555')
                        expect(ops[1].patch).to.deep.equal(patch_c)
                    })
            })
    })
    
    it('can delete a record', function ()
    {
        var orig_doc = null
        
        return monsters
            .create({ name: 'werewolf', height: 1.2, scariness: 7.3256 }, '123')
            .then(function (doc)
            {
                orig_doc = doc
                return monsters.delete(doc.id, '555')
            })
            .then(function (doc)
            {
                expect(doc).to.deep.equal(orig_doc)
                return get_db().table('monsters_ops')
                    .getAll(doc.id, {index: 'docid'})
                    .run()
                    .then(function (ops)
                    {
                        ops = ops.sort(function (a, b)
                        {
                            return a.version - b.version
                        })
                        
                        expect(ops.length).to.equal(2)
                        expect(ops[0].version).to.equal(0)
                        expect(ops[0].created).to.equal(1)
                        expect(ops[1].version).to.equal(1)
                        expect(ops[1].when).to.be.instanceof(Date)
                        expect(ops[1].user_id).to.equal('555')
                        expect(ops[1].deleted).to.equal(1)
                    })
            })
    })
    
    it('can return a single record', function ()
    {
        var werewolf = null
        
        return monsters
            .create({ name: 'werewolf', height: 1.2, scariness: 7.3256 }, '123')
            .then(function (doc)
            {
                werewolf = doc
                return monsters.create({ name: 'kraken', height: 14, scariness: 82.9 }, '123')
            })
            .then(function ()
            {
                monsters.get(werewolf.id).then(function (doc)
                {
                    expect(doc).to.deep.equal(werewolf)
                })
            })
    })
    
    it('can get a list of indexes', function ()
    {
        return monsters.indexes().then(function (indexes)
        {
            expect(indexes).to.have.members(['height', 'scariness'])
        })
    })
    
    it('can lookup items by indexed value', function ()
    {
        return Promise.all([
            monsters.create({ name: 'tiny bat', height: 0.083, scariness: 0.01 }),
            monsters.create({ name: 'large bat', height: 0.3, scariness: 0.1 }),
            monsters.create({ name: 'minotaur', height: 1.6, scariness: 6 }),
            monsters.create({ name: 'kishi', height: 1.6, scariness: 7 }),
            monsters.create({ name: 'kraken', height: 14, scariness: 82.9 }),
            monsters.create({ name: 'werekraken', height: 14, scariness: 1000.002 })
        ])
        .then(function ()
        {
            return monsters
                .find({ value: 14, index: 'height' })
                .then(function (docs)
                {
                    var names = docs.map(function (doc)
                    {
                        return doc.name
                    }).sort()
                    
                    expect(names).to.deep.equal(['kraken', 'werekraken'])
                })
        })
    })
    
    it('can lookup items by a range of indexed values', function ()
    {
        return Promise.all([
            monsters.create({ name: 'tiny bat', height: 0.083, scariness: 0.01 }),
            monsters.create({ name: 'large bat', height: 0.3, scariness: 0.1 }),
            monsters.create({ name: 'minotaur', height: 1.6, scariness: 6 }),
            monsters.create({ name: 'kishi', height: 1.6, scariness: 7 }),
            monsters.create({ name: 'kraken', height: 14, scariness: 82.9 }),
            monsters.create({ name: 'werekraken', height: 14, scariness: 1000.002 })
        ])
        .then(function ()
        {
            return monsters
                .find({ start: 4, end: 20, index: 'scariness' })
                .then(function (docs)
                {
                    var names = docs.map(function (doc)
                    {
                        return doc.name
                    }).sort()
                    
                    expect(names).to.deep.equal(['kishi', 'minotaur'])
                })
        })
    })

    it('can watch changes on a single item', function ()
    {
        var ops = [],
            orig_doc = null,
            patch_a = null,
            patch_b = null
        
        return monsters
            .create({ name: 'werewolf', height: 1.2, scariness: 7.3256 })
            .then(function (doc)
            {
                orig_doc = doc
                return monsters.watch_one(doc.id)
            })
            .then(function (cursor)
            {
                cursor.each(function (err, op)
                {
                    if (err)
                    {
                        throw err
                    }
                    
                    ops.push(op)
                })
                
                var update_a = shallow_copy(orig_doc)
                update_a.height = 1.4
                patch_a = jiff.diff(orig_doc, update_a)
                
                return monsters.patch(orig_doc.id, orig_doc.version, patch_a)
            })
            .then(function (updated_doc)
            {
                var update_b = shallow_copy(updated_doc)
                update_b.height = 1.4
                patch_b = jiff.diff(updated_doc, update_b)
                return monsters.patch(updated_doc.id, updated_doc.version, patch_b)
            })
            .then(function ()
            {
                expect(ops.length).to.equal(2)
                expect(ops[0].docid).to.equal(orig_doc.id)
                expect(ops[0].version).to.equal(1)
                expect(ops[0].patch).to.deep.equal(patch_a)
                expect(ops[1].docid).to.equal(orig_doc.id)
                expect(ops[1].version).to.equal(2)
                expect(ops[1].patch).to.deep.equal(patch_b)
            })
    })
    
    it('can watch changes on multiple items', function ()
    {
        var ops = [],
            docs = [],
            push_doc = docs.push.bind(docs),
            patch_4 = null
        
        return monsters
            .create({ name: 'tiny bat', height: 0.083, scariness: 0.01 }).then(push_doc)
            .then(function ()
            {
                return monsters.create({ name: 'large bat', height: 0.3, scariness: 0.1 }).then(push_doc)
            })
            .then(function ()
            {
                return monsters.create({ name: 'minotaur', height: 1.6, scariness: 6 }).then(push_doc)
            })
            .then(function ()
            {
                return monsters.create({ name: 'kishi', height: 1.6, scariness: 7 }).then(push_doc)
            })
            .then(function ()
            {
                return monsters.create({ name: 'kraken', height: 14, scariness: 82.9 }).then(push_doc)
            })
            .then(function ()
            {
                return monsters.create({ name: 'werekraken', height: 14, scariness: 1000.002 }).then(push_doc)
            })
            .then(function ()
            {
                return monsters.watch({ value: 14, index: 'height' })
            })
            .then(function (cursor)
            {
                cursor.each(function (err, op)
                {
                    if (err)
                    {
                        throw err
                    }
                    
                    ops.push(op)
                })
                
                var updated_4 = shallow_copy(docs[4])
                updated_4.scariness = 0.5
                patch_4 = jiff.diff(docs[4], updated_4)
                
                return monsters
                    .patch(updated_4.id, updated_4.version, patch_4)
                    .then(function ()
                    {
                        return monsters.create({ name: 'basilisk', height: 14, scariness: 37 }).then(push_doc)
                    })
                    .then(function ()
                    {
                        return monsters.delete(docs[5].id)
                    })
                    .then(function ()
                    {
                        return monsters.delete(docs[0].id)
                    })
            })
            .then(function ()
            {
                expect(ops.length).to.equal(3)
                expect(ops[0].docid).to.equal(docs[4].id)
                expect(ops[0].version).to.equal(docs[4].version)
                expect(ops[0].patch).to.deep.equal(patch_4)
                expect(ops[1].docid).to.equal(docs[6].id)
                expect(ops[1].version).to.equal(0)
                expect(ops[1].created).to.equal(1)
                expect(ops[2].docid).to.equal(docs[5].id)
                expect(ops[2].version).to.equal(docs[5].version)
                expect(ops[2].deleted).to.equal(1)
            })
    })
})