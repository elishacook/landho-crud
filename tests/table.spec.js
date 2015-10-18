'use strict'

var r = require('rethinkdb'),
    db = require('../lib/db'),
    Table = require('../lib/table'),
    Promise = require('bluebird'),
    async = require('async'),
    db_options = {
        host: 'localhost',
        port: 28015,
        
        db: 'landho_crud_test'
    },
    copy = require('../lib/shallow-copy'),
    monsters = null,
    SyncDocument = require('sync-document')

function handle_error(err)
{
    if (err)
    {
        throw err
    }
}

function sort_monsters(monsters)
{
    return monsters.sort(function (a, b)
    {
        return (a.name > b.name ? 1 : -1)
    })
}

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
    
    it('adds table to db', function ()
    {
        db.tables = []
        var foos = new Table('foos', ['things', 'stuff'])
        
        expect(db.tables.length).to.equal(1)
        
        expect(db.tables[0].name).to.equal('foos')
        expect(db.tables[0].indexes.length).to.equal(2)
        expect(db.tables[0].indexes[0]).to.equal('things')
        expect(db.tables[0].indexes[1]).to.equal('stuff')
    })
    
    it('can create a new record', function (done)
    {
        return monsters.create(
            { name: 'werewolf', height: 1.2, scariness: 7.3256 },
            function (err, doc)
            {
                handle_error(err)
                expect(doc.id).to.not.be.undefined
                expect(doc.name).to.equal('werewolf')
                expect(doc.height).to.equal(1.2)
                expect(doc.scariness).to.equal(7.3256)
                done()
            })  
    })
    
    it('can update a record', function (done)
    {
        return monsters.create(
            { name: 'werewolf', height: 1.2, scariness: 7.3256 },
            function (err, orig_doc)
            {
                handle_error(err)
                var updated_doc = copy(orig_doc)
                updated_doc.height = 1.7
                monsters.update(updated_doc, function (err, doc)
                {
                    handle_error(err)
                    expect(doc.id).to.equal(orig_doc.id)
                    expect(doc.height).to.equal(1.7)
                    done()
                })
            }
        )
    })
    
    it('can delete a record', function (done)
    {
        return monsters.create(
            { name: 'werewolf', height: 1.2, scariness: 7.3256 },
            function (err, orig_doc)
            {
                handle_error(err)
                monsters.delete(orig_doc.id, function (err, doc)
                {
                    handle_error(err)
                    expect(doc).to.deep.equal(orig_doc)
                    done()
                })
            }
        )
    })
    
    it('can return a single record', function (done)
    {
        return monsters.create(
            { name: 'werewolf', height: 1.2, scariness: 7.3256 },
            function (err, werewolf)
            {
                handle_error(err)
                monsters.get(werewolf.id, function (err, doc)
                {
                    handle_error(err)
                    expect(doc).to.deep.equal(werewolf)
                    done()
                })
            }
        )
    })
    
    it('can get a list of indexes', function (done)
    {
        return monsters.indexes(function (err, indexes)
        {
            handle_error(err)
            expect(indexes).to.have.members(['height', 'scariness'])
            done()
        })
    })
    
    it('can lookup items by indexed value', function (done)
    {
        async.parallel([
            monsters.create.bind(monsters, { name: 'tiny bat', height: 0.083, scariness: 0.01 }),
            monsters.create.bind(monsters, { name: 'large bat', height: 0.3, scariness: 0.1 }),
            monsters.create.bind(monsters, { name: 'minotaur', height: 1.6, scariness: 6 }),
            monsters.create.bind(monsters, { name: 'kishi', height: 1.6, scariness: 7 }),
            monsters.create.bind(monsters, { name: 'kraken', height: 14, scariness: 82.9 }),
            monsters.create.bind(monsters, { name: 'werekraken', height: 14, scariness: 1000.002 })
        ], function (err, results)
        {
            handle_error(err)
            monsters.find(
                { value: 14, index: 'height' },
                function (err, docs)
                {
                    handle_error(err)
                    var names = docs.map(function (doc)
                    {
                        return doc.name
                    }).sort()
                    
                    expect(names).to.deep.equal(['kraken', 'werekraken'])   
                    done()
                }
            )
        })
    })
    
    it('can lookup items by a range of indexed values', function (done)
    {
        async.parallel([
            monsters.create.bind(monsters, { name: 'tiny bat', height: 0.083, scariness: 0.01 }),
            monsters.create.bind(monsters, { name: 'large bat', height: 0.3, scariness: 0.1 }),
            monsters.create.bind(monsters, { name: 'minotaur', height: 1.6, scariness: 6 }),
            monsters.create.bind(monsters, { name: 'kishi', height: 1.6, scariness: 7 }),
            monsters.create.bind(monsters, { name: 'kraken', height: 14, scariness: 82.9 }),
            monsters.create.bind(monsters, { name: 'werekraken', height: 14, scariness: 1000.002 })
        ], 
        function (err, results)
        {
            handle_error(err)
            monsters.find(
                { start: 4, end: 20, index: 'scariness' },
                function (err, docs)
                {
                    handle_error(err)
                    var names = docs.map(function (doc)
                    {
                        return doc.name
                    }).sort()
                    
                    expect(names).to.deep.equal(['kishi', 'minotaur'])
                    done()
                }
            )
        })
    })
    
    it('can watch changes on a single item', function (done)
    {
        monsters.create(
            { name: 'werewolf', height: 1.2, scariness: 7.3256 },
            function (err, doc)
            {
                handle_error(err)
                monsters.get_and_watch(doc.id, function (err, channel)
                {
                    handle_error(err)
                    
                    var orig_doc = null,
                        updated_doc = null,
                        history = [],
                        channel = channel.end,
                        finish = function ()
                        {
                            expect(history.length).to.equal(3)
                            expect(history[0][0]).to.equal('initial')
                            expect(history[0][1]).to.deep.equal(orig_doc)
                            expect(history[1][0]).to.equal('update')
                            expect(history[1][1]).to.deep.equal(updated_doc)
                            expect(history[2][0]).to.equal('delete')
                            expect(history[2][1]).to.equal(updated_doc.id)
                            
                            channel.close()
                            done()
                        }
                    
                    channel.on('error', handle_error)
                    
                    channel.on('initial', function (doc)
                    {
                        orig_doc = copy(doc)
                        history.push(['initial', orig_doc])
                        doc.height = 1.7
                        monsters.update(doc, function (err, doc)
                        {
                            handle_error(err)
                            updated_doc = doc
                        })
                    })
                    
                    channel.on('update', function (doc)
                    {
                        history.push(['update', doc])
                        monsters.delete(doc.id, handle_error)
                    })
                    
                    channel.on('delete', function (doc)
                    {
                        history.push(['delete', doc])
                        setTimeout(finish, 20)
                    })
                })
            }
        )
    })
    
    it('can watch changes on multiple items', function (done)
    {
        async.parallel([
            monsters.create.bind(monsters, { name: 'tiny bat', height: 0.083, scariness: 0.01 }),
            monsters.create.bind(monsters, { name: 'large bat', height: 0.3, scariness: 0.1 }),
            monsters.create.bind(monsters, { name: 'minotaur', height: 1.6, scariness: 6 }),
            monsters.create.bind(monsters, { name: 'kishi', height: 1.6, scariness: 7 }),
            monsters.create.bind(monsters, { name: 'kraken', height: 14, scariness: 82.9 }),
            monsters.create.bind(monsters, { name: 'werekraken', height: 14, scariness: 1000.002 })
        ], function (err, result)
        {
            handle_error(err)
            monsters.find_and_watch(
                { start: 14, end: 20, index: 'height' },
                function (err, channel)
                {
                    handle_error(err)
                    
                    var channel = channel.end
                    channel.on('error', handle_error)
                    
                    var history = []
                    ;['insert', 'update', 'delete'].forEach(function (k)
                    {
                        channel.on(k, function (value)
                        {
                            history.push([k, value])
                        })
                    })
                    
                    channel.on('initial', function (original_docs)
                    {
                        history.push(['initial', sort_monsters(original_docs)])
                        
                        var update_one = copy(original_docs[0]),
                            update_two = copy(original_docs[1])
                        
                        update_one.scariness = 666
                        update_two.height = 1
                        
                        async.series([
                            monsters.create.bind(monsters, { name: 'not a monster', height: 0.0023, scariness: 0 }),
                            monsters.create.bind(monsters, { name: 'impalerbot', height: 17, scariness: 77 }),
                            monsters.update.bind(monsters, update_one),
                            monsters.update.bind(monsters, update_two),
                            monsters.delete.bind(monsters, update_one.id)
                        ],
                        function (err)
                        {
                            handle_error(err)
                            
                            expect(history.length).to.equal(5)
                            
                            expect(history[0][0]).to.equal('initial')
                            expect(history[0][1].length).to.equal(2)
                            expect(history[0][1][0].name).to.equal('kraken')
                            expect(history[0][1][1].name).to.equal('werekraken')
                            
                            expect(history[1][0]).to.equal('insert')
                            expect(history[1][1].name).to.equal('impalerbot')
                            
                            expect(history[2][0]).to.equal('update')
                            expect(history[2][1].name).to.equal(update_one.name)
                            expect(history[2][1].scariness).to.equal(update_one.scariness)
                            
                            expect(history[3][0]).to.equal('delete')
                            expect(history[3][1]).to.equal(update_two.id)
                            
                            expect(history[4][0]).to.equal('delete')
                            expect(history[4][1]).to.equal(update_one.id)
                            
                            channel.close()
                            done()
                        })
                    })
                }
            )
        })
    })
    
    it('can sync with a single item', function (done)
    {
        monsters.create(
            { name: 'werewolf', height: 1.2, scariness: 7.3256 },
            function (err, doc)
            {
                handle_error(err)
                monsters.get_and_sync(doc.id, function (err, channel)
                {
                    handle_error(err)
                    var channel = channel.end
                    
                    channel.on('error', handle_error)
                    
                    var syncdoc = null
                    
                    channel.on('pull', function (edits)
                    {
                        edits.forEach(function (edit)
                        {
                            syncdoc.pull(edit)
                        })
                    })
                    
                    channel.on('delete', function ()
                    {
                        syncdoc.deleted = true
                    })
                    
                    var update_one = copy(doc)
                    update_one.height = 555
                    var update_two = copy(update_one)
                    update_two.scariness = 111
                    
                    
                    channel.on('initial', function (doc)
                    {
                        syncdoc = new SyncDocument(doc)
                        
                        async.series([
                            function (done)
                            {
                                syncdoc.object = update_one
                                syncdoc.push()
                                channel.emit('pull', syncdoc.edits)
                                done()
                            },
                            monsters.update.bind(monsters, update_two),
                            monsters.get.bind(monsters, doc.id),
                            monsters.delete.bind(monsters, doc.id)
                        ],
                        function (err, results)
                        {
                            handle_error(err)
                            
                            var updated_doc = results[results.length-2]
                            expect(syncdoc.object).to.deep.equal(updated_doc)
                            expect(updated_doc.id).to.equal(doc.id)
                            expect(updated_doc.height).to.equal(555)
                            expect(updated_doc.scariness).to.equal(111)
                            expect(syncdoc.deleted).to.be.true
                            
                            channel.close()
                            
                            done()
                        })
                    })
                })
            }
        )
    })
    
    it('can sync with a list of items', function (done)
    {
        async.parallel([
            monsters.create.bind(monsters, { name: 'tiny bat', height: 0.083, scariness: 0.01 }),
            monsters.create.bind(monsters, { name: 'large bat', height: 0.3, scariness: 0.1 }),
            monsters.create.bind(monsters, { name: 'minotaur', height: 1.6, scariness: 6 }),
            monsters.create.bind(monsters, { name: 'kishi', height: 1.6, scariness: 7 }),
            monsters.create.bind(monsters, { name: 'kraken', height: 14, scariness: 82.9 }),
            monsters.create.bind(monsters, { name: 'werekraken', height: 14, scariness: 1000.002 })
        ], function (err, result)
        {
            handle_error(err)
            monsters.find_and_sync(
                { start: 14, end: 20, index: 'height' },
                function (err, channel)
                {
                    handle_error(err)
                    
                    var channel = channel.end
                    channel.on('error', handle_error)
                    
                    var syncdocs = {},
                        original_docs = []
                    
                    channel.on('pull', function (id, edits)
                    {
                        var syncdoc = syncdocs[id]
                        
                        edits.forEach(function (edit)
                        {
                            syncdoc.pull(edit)
                        })
                    })
                    
                    channel.on('insert', function (doc)
                    {
                        syncdocs[doc.id] = new SyncDocument(doc)
                        original_docs.push(doc)
                    })
                    
                    channel.on('delete', function (id)
                    {
                        syncdocs[id].deleted = true
                    })
                    
                    channel.on('initial', function (results)
                    {
                        original_docs = sort_monsters(results)
                        original_docs.forEach(function (doc)
                        {
                            syncdocs[doc.id] = new SyncDocument(doc)
                        })
                        
                        var update_one = copy(original_docs[0]),
                            update_two = copy(original_docs[1])
                        
                        update_one.scariness = 666
                        update_two.height = 1
                        
                        async.series([
                            monsters.create.bind(monsters, { name: 'not a monster', height: 0.0023, scariness: 0 }),
                            monsters.create.bind(monsters, { name: 'impalerbot', height: 17, scariness: 77 }),
                            function (done)
                            {
                                var syncdoc = syncdocs[update_one.id]
                                syncdoc.object = update_one
                                syncdoc.push()
                                channel.emit('pull', { id: update_one.id, edits: syncdoc.edits })
                                done()
                            },
                            monsters.update.bind(monsters, update_two),
                            monsters.delete.bind(monsters, update_one.id),
                            monsters.find.bind(monsters, { start: 14, end: 20, index: 'height' })
                        ],
                        function (err, results)
                        {
                            handle_error(err)
                            
                            var final_results = sort_monsters(results[results.length-1])
                            
                            expect(original_docs.length).to.equal(3)
                            expect(final_results.length).to.equal(1)
                            
                            expect(original_docs[0].name).to.equal('kraken')
                            expect(original_docs[0].scariness).to.equal(666)
                            expect(syncdocs[original_docs[0].id].deleted).to.be.true
                            
                            expect(original_docs[1].name).to.equal('werekraken')
                            expect(syncdocs[original_docs[1].id].deleted).to.be.true
                            
                            expect(original_docs[2].name).to.equal('impalerbot')
                            expect(original_docs[2]).to.deep.equal(final_results[0])
                            
                            channel.close()
                            done()
                        })
                    })
                }
            )
        })
    })
})