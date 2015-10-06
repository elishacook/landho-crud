'use strict'

var expect = require('chai').expect,
    r = require('rethinkdb'),
    db_options = {
        host: 'localhost',
        port: 28015,
        db: 'landho_crud_test'
    },
    api = require('landho')(),
    crud = require('../lib/index')(api, db_options),
    EventEmitter = require('events')
    
function setup()
{
    crud(
    {
        name: 'foos',
        schema: {
            things: { type: String },
            dingbats: { type: Array, items: { type: Number } }
        },
        indexes: ['things', 'dingbats']
    })
    
    crud(
    {
        name: 'bars',
        schema: {
            stuff: { type: Number }
        }
    })
    
    return crud.init()
}

function clear_tables ()
{
    return crud.db.r.table('foos').delete().run()
    .then(function ()
    {
        return crud.db.r.table('bars').delete().run()
    })
}

describe('landho-crud', function ()
{
    before(setup)
    beforeEach(clear_tables)
    
    it('creates tables based on service descriptions', function ()
    {
        return crud.db.r.tableList().run()
            .then(function (tables)
            {
                expect(tables.sort()).to.deep.equal(['bars', 'foos'])
            })
            .then(function ()
            {
                return crud.db.r.table('foos').indexList().run()
            })
            .then(function (foos_indexes)
            {
                expect(foos_indexes.sort()).to.deep.equal(['dingbats', 'things'])
            })
    })
    
    it('adds services to the landho instance', function ()
    {
        expect(api.service('foos')).to.not.be.undefined
        expect(api.service('bars')).to.not.be.undefined
    })
    
    it('has a create method', function ()
    {
        expect(api.service('foos').create).to.not.be.undefined
    })
    
    it('validates against the schema before calling create()', function (done)
    {
        api.service('foos').create({ data: { things: [1,2,3] } }, function (err)
        {
            expect(err.code).to.equal(400)
            expect(err.message).to.equal('Validation error')
            expect(err.errors.things).to.equal('Expected an instance of function String() { [native code] }')
            done()
        })
    })
    
    it('can add record to the database', function (done)
    {
        api.service('foos').create(
        {
            data:
            {
                things: 'hats',
                dingbats: [3,2,1]
            }
        },
        function (err, result)
        {
            crud.db.r.table('foos').get(result.id).run().then(function (record)
            {
                expect(record.id).to.not.be.undefined
                expect(record.things).to.equal('hats')
                expect(record.dingbats).to.deep.equal([3,2,1])
                done()
            })
        })
    })
    
    it('adds created and modified dates to new records', function (done)
    {
        api.service('foos').create(
        {
            data:
            {
                things: 'hats',
                dingbats: [3,2,1]
            }
        },
        function (err, result)
        {
            expect(result.modified).to.not.be.undefined
            expect(result.created).to.not.be.undefined
            expect(result.created.toString()).to.equal(result.modified.toString())
            expect(result.created.constructor).to.equal(Date)
            done()
        })
    })
    
    it('has an update method', function ()
    {
        expect(api.service('foos').update).to.not.be.undefined
    })
    
    it('validates the record before calling update()', function (done)
    {
        api.service('foos').create(
        {
            data:
            {
                things: 'hats',
                dingbats: [3,2,1]
            }
        },
        function (err, result)
        {
            api.service('foos').update({ data: {} }, function (err)
            {
                expect(err).to.not.be.null
                expect(err.code).to.equal(400)
                expect(err.message).to.equal('Validation error')
                expect(err.errors.id).to.equal('This field is required')
                done()
            })
        })
    })
    
    it('updates the modified date before calling update()', function (done)
    {
        api.service('foos').create(
        {
            data:
            {
                things: 'hats',
                dingbats: [3,2,1]
            }
        },
        function (err, original_record)
        {
            api.service('foos').update(
            {
                data:
                {
                    id: original_record.id,
                    created: original_record.created,
                    things: 'shoes',
                    dingbats: [1,2,3]
                }
            },
            function (err, result)
            {
                crud.db.r.table('foos').get(original_record.id).run().then(function (updated_record)
                {
                    expect(updated_record.id).to.equal(original_record.id)
                    expect(updated_record.things).to.equal('shoes')
                    expect(updated_record.dingbats).to.deep.equal([1,2,3])
                    expect(updated_record.modified).to.be.greaterThan(original_record.modified)
                    done()
                })
            })
        })
    })
    
    it('can remove a record from the database', function (done)
    {
        var foos = api.service('foos')
        
        foos.create({ data: { things: 'a' } },
        function (err, record_a)
        {
            foos.create({ data: { things: 'b' } },
            function (err, record_b)
            {
                foos.remove({ data: { id: record_a.id } }, function (err, deleted_record)
                {
                    expect(deleted_record.id).to.equal(record_a.id)
                    
                    crud.db.r.table('foos').get(deleted_record.id).run().then(function (result)
                    {
                        expect(result).to.be.null
                        done()
                    })
                })
            })
        })
    })
    
    it('has a get method that returns a single record', function (done)
    {
        var foos = api.service('foos')
        
        foos.create({ data: { things: 'a' } },
        function (err, record_a)
        {
            foos.create({ data: { things: 'b' } },
            function (err, record_b)
            {
                foos.get({ data: { id: record_a.id } }, function (err, record)
                {
                    expect(record).to.deep.equal(record_a)
                    done()
                })
            })
        })
    })
    
    it('can provide a feed for changes to a single record', function (done)
    {
        var foos = api.service('foos'),
            subscriber = new EventEmitter(),
            record = null,
            feed = null,
            calls = {
                initial: false,
                update: false
            }
        
        subscriber.on('initial', function (result)
        {
            expect(result).to.deep.equal(record)
            calls.initial = true
        })
        
        subscriber.on('update', function (result)
        {
            expect(calls.initial).to.be.true
            expect(result.things).to.equal('b')
            calls.update = true
        })
        
        subscriber.on('remove', function (result)
        {
            expect(calls.initial).to.be.true
            expect(calls.update).to.be.true
            expect(result.id).to.equal(record.id)
            feed.close()
            done()
        })
        
        foos.create(
            { data: { things: 'a' } },
            function (err, result)
            {
                record = result
                
                foos.get(
                    {
                        data: { id: result.id },
                        subscriber: subscriber
                    },
                    function (err, f)
                    {
                        feed = f
                        
                        foos.update(
                            { data: { created: result.created, id: result.id, things: 'b' } }, 
                            function (err, result)
                            {
                                foos.remove({ data: { id: result.id } }, function (){})
                            }
                        )
                    }
                )
            }
        )
    })
    
    var setup_foos_records = function (done)
    {
        var foos = api.service('foos')
        
        foos.create({ data: { things: 'a' } }, function ()
        {
            foos.create({ data: { things: 'b' } }, function ()
            {
                foos.create({ data: { things: 'c' } }, function ()
                {
                    done(foos)
                })
            })
        })
    }
    
    it('has a find method that returns a list of records', function (done)
    {
        setup_foos_records(function (foos)
        {
            foos.find({}, function (err, results)
            {
                expect(results.length).to.equal(3)
                done()
            })
        })
    })
    
    it('can limit find results', function (done)
    {
        setup_foos_records(function (foos)
        {
            foos.find({ data: { limit: 2 } }, function (err, results)
            {
                expect(results.length).to.equal(2)
                done()
            })
        })
    })
    
    it('can skip find results', function (done)
    {
        setup_foos_records(function (foos)
        {
            foos.find({ data: { skip: 2 } }, function (err, results)
            {
                expect(results.length).to.equal(1)
                done()
            })
        })
    })
    
    it('can order find results', function (done)
    {
        setup_foos_records(function (foos)
        {
            foos.find({ data: { orderBy: 'things' } }, function (err, results)
            {
                expect(results.length).to.equal(3)
                expect(results[0].things).to.equal('a')
                expect(results[1].things).to.equal('b')
                expect(results[2].things).to.equal('c')
                done()
            })
        })
    })
    
    it('can filter find results by field value', function (done)
    {
        setup_foos_records(function (foos)
        {
            foos.find({ data: { filter: { things: 'a' } } }, function (err, results)
            {
                expect(results.length).to.equal(1)
                expect(results[0].things).to.equal('a')
                done()
            })
        })
    })
    
    it('can provide a feed of changes to find results', function (done)
    {
        setup_foos_records(function (foos)
        {
            var subscriber = new EventEmitter(),
                feed = null,
                original_results = null
            
            subscriber.on('initial', function (result)
            {
                original_results = result
                expect(result.length).to.equal(3)
                
                result[0].things = 'THINGS!'
                
                foos.update({ data: result[0] }, function () {})
            })
            
            subscriber.on('update', function (result)
            {
                expect(result.id).to.equal(original_results[0].id)
                expect(result.things).to.equal('THINGS!')
                
                foos.remove({ data: { id: result.id } }, function () {})
            })
            
            subscriber.on('remove', function (result)
            {
                expect(result.id).to.equal(original_results[0].id)
                foos.create({ data: { things: 'Imma new' } }, function () {})
            })
            
            subscriber.on('append', function (result)
            {
                expect(result.things).to.equal('Imma new')
                feed.close()
                done()
            })
            
            foos.find({ subscriber: subscriber }, function (err, f)
            {
                feed = f
            })
        })
    })
})