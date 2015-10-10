'use strict'

var landho = require('landho'),
    landho_crud = require('../lib/index'),
    api = null,
    crud = null,
    EventEmitter = require('events')

function FakeTable (name, indexes)
{
    this.name = name
    this.indexes = indexes
}

describe('landho-crud', function ()
{
    beforeEach(function ()
    {
        api = landho()
        crud = landho_crud(api)
    })
    
    it('creates a table object', function ()
    {
        var service = crud(
            {
                name: 'foos',
                schema: {},
                indexes: ['bob'],
                table_cls: FakeTable
            })
        
        expect(service.table).to.be.instanceof(FakeTable)
        expect(service.table.name).to.equal('foos')
        expect(service.table.indexes).to.deep.equal(['bob'])
    })
    
    describe('create()', function ()
    {
        it('resolves with an error if validation fails', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: { things: { type: Array, required: true } },
                indexes: ['bob'],
                table_cls: FakeTable
            })
            
            service.create({ data: {} }, function (err, result)
            {
                expect(err).to.not.be.null
                expect(err.code).to.equal(400)
                expect(err.message).to.equal('Validation error')
                expect(err.errors.things).to.equal('This field is required')
                done()
            })
        })
        
        it('calls the table create method when validation passes', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: { things: { type: Array, required: true } },
                indexes: ['bob'],
                table_cls: FakeTable
            })
            
            service.table.create = sinon.stub().resolves('a new foo')
            
            service.create({ data: { things: [] }, user: { id: '123' } }, function (err, result)
            {
                expect(err).to.be.null
                expect(result).to.equal('a new foo')
                expect(service.table.create).to.have.been.calledOnce
                expect(service.table.create).to.have.been.calledWith({ things: [] }, '123')
                done()
            })
        })
        
        it('resolves with an error if table.create creates an error', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: { things: { type: Array, required: true } },
                indexes: ['bob'],
                table_cls: FakeTable
            })
            
            service.table.create = sinon.stub().rejects('AAAARGH!')
            
            service.create({ data: { things: [] }, user: { id: '123' } }, function (err, result)
            {
                expect(err.message).to.equal('AAAARGH!')
                done()
            })
        })
    })
    
    describe('update()', function ()
    {
        it('resolves with an error if validation fails', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: { things: { type: Array, required: true } },
                indexes: ['bob'],
                table_cls: FakeTable
            })
            
            service.update({ data: {} }, function (err, result)
            {
                expect(err).to.not.be.null
                expect(err.code).to.equal(400)
                expect(err.message).to.equal('Validation error')
                expect(err.errors.id).to.equal('This field is required')
                expect(err.errors.things).to.equal('This field is required')
                done()
            })
        })
        
        it('calls the table update method when validation passes', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: { things: { type: Array, required: true } },
                indexes: ['bob'],
                table_cls: FakeTable
            })
            
            service.table.update = sinon.stub().resolves('an updated foo')
            
            service.update({ data: { id: '666', things: [] }, user: { id: '123' } }, function (err, result)
            {
                expect(err).to.be.null
                expect(result).to.equal('an updated foo')
                expect(service.table.update).to.have.been.calledOnce
                expect(service.table.update).to.have.been.calledWith({ id: '666', things: [] }, '123')
                done()
            })
        })
        
        it('resolves with an error if table.update creates an error', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: { things: { type: Array, required: true } },
                indexes: ['bob'],
                table_cls: FakeTable
            })
            
            service.table.update = sinon.stub().rejects('AAAARGH!')
            
            service.update({ data: { id: '666', things: [] }, user: { id: '123' } }, function (err, result)
            {
                expect(err.message).to.equal('AAAARGH!')
                done()
            })
        })
    })
    
    describe('delete()', function ()
    {
        it('resolves with an error if validation fails', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: {},
                table_cls: FakeTable
            })
            
            service.delete({ data: {} }, function (err, result)
            {
                expect(err).to.not.be.null
                expect(err.code).to.equal(400)
                expect(err.message).to.equal('Validation error')
                expect(err.errors.id).to.equal('This field is required')
                done()
            })
        })
        
        it('calls the table update method when validation passes', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: {},
                table_cls: FakeTable
            })
            
            service.table.delete = sinon.stub().resolves('an updated foo')
            
            service.delete({ data: { id: '666' }, user: { id: '123' } }, function (err, result)
            {
                expect(err).to.be.null
                expect(result).to.equal('an updated foo')
                expect(service.table.delete).to.have.been.calledOnce
                expect(service.table.delete).to.have.been.calledWith('666', '123')
                done()
            })
        })
        
        it('resolves with an error if table.delete creates an error', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: {},
                table_cls: FakeTable
            })
            
            service.table.delete = sinon.stub().rejects('AAAARGH!')
            
            service.delete({ data: { id: '666' }, user: { id: '123' } }, function (err, result)
            {
                expect(err.message).to.equal('AAAARGH!')
                done()
            })
        })
    })

    describe('get()', function ()
    {
        it('calls table.get in request/response mode', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: {},
                table_cls: FakeTable
            })
            
            service.table.get = sinon.stub().resolves('some document')
            
            service.get({ data: { id: '123' } }, function (err, result)
            {
                expect(err).to.be.null
                expect(result).to.equal('some document')
                expect(service.table.get).to.have.been.calledOnce
                expect(service.table.get).to.have.been.calledWith('123')
                done()
            })
        })
        
        it('resolves with an error if table.get creates an error', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: {},
                table_cls: FakeTable
            })
            
            service.table.get = sinon.stub().rejects('some error')
            
            service.get({ data: { id: '123' } }, function (err, result)
            {
                expect(err.message).to.equal('some error')
                done()
            })
        })
        
        it('calls table.watch_one in feed mode', function (done)
        {
            var subscriber = { emit: sinon.stub() },
                cursor = { each: sinon.stub(), close: sinon.stub() },
                service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                })
            
            service.table.get = sinon.stub().resolves('initial value')
            service.table.watch_one = sinon.stub().resolves(cursor)
            
            service.get({ data: { id: '123' }, subscriber: subscriber }, function (err, result)
            {
                expect(err).to.be.null
                expect(service.table.get).to.have.been.calledOnce
                expect(service.table.get).to.have.been.calledWith('123')
                expect(service.table.watch_one).to.have.been.calledOnce
                expect(service.table.watch_one).to.have.been.calledWith('123')
                done()
            })
        })
        
        it('resolves with an error if table.watch_one creates an error', function (done)
        {
            var service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                }),
                subscriber = {
                    emit: sinon.stub()
                }
            
            service.table.get = sinon.stub().resolves('initial value')
            service.table.watch_one = sinon.stub().rejects('some error')
            
            service.get({ data: { id: '123' }, subscriber: subscriber }, function (err, result)
            {
                expect(err.message).to.equal('some error')
                done()
            })
        })
        
        it('publishes changes to its subscriber', function (done)
        {
            var subscriber = { emit: sinon.stub() },
                cursor = { each: sinon.stub(), close: sinon.stub() },
                service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                })
            
            service.table.get = sinon.stub().resolves('initial value')
            service.table.watch_one = sinon.stub().resolves(cursor)
            
            service.get({ data: { id: '123' }, subscriber: subscriber }, function (err, result)
            {
                expect(cursor.each).to.have.been.calledOnce
                expect(cursor.each.firstCall.args[0]).to.be.instanceof(Function)
                
                cursor.each.firstCall.args[0](null, 'some change')
                
                expect(subscriber.emit).to.have.been.calledTwice
                expect(subscriber.emit.firstCall.args).to.deep.equal(['initial', 'initial value'])
                expect(subscriber.emit.secondCall.args).to.deep.equal(['change', 'some change'])
                
                done()
            })
        })

        it('publishes errors to its subscriber', function (done)
        {
            var subscriber = { emit: sinon.stub() },
                cursor = { each: sinon.stub(), close: sinon.stub() },
                service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                })
            
            service.table.get = sinon.stub().resolves('initial value')
            service.table.watch_one = sinon.stub().resolves(cursor)
            
            service.get({ data: { id: '123' }, subscriber: subscriber }, function (err, result)
            {
                expect(cursor.each).to.have.been.calledOnce
                expect(cursor.each.firstCall.args[0]).to.be.instanceof(Function)
                
                cursor.each.firstCall.args[0]({ message: 'some error' })
                
                expect(subscriber.emit).to.have.been.calledTwice
                expect(subscriber.emit.args[0].length).to.equal(2)
                expect(subscriber.emit.args[0][0]).to.equal('initial')
                expect(subscriber.emit.args[0][1]).to.equal('initial value')
                expect(subscriber.emit.args[1].length).to.equal(2)
                expect(subscriber.emit.args[1][0]).to.equal('error')
                expect(subscriber.emit.args[1][1].code).to.equal(500)
                expect(subscriber.emit.args[1][1].message).to.equal('some error')
                
                done()
            })
        })
    })
    
    describe('find()', function ()
    {
        it('calls table.find in request/response mode', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: {},
                table_cls: FakeTable
            })
            
            service.table.find = sinon.stub().resolves('response')
            
            service.find({ data: { index: 'some index' } }, function (err, result)
            {
                expect(err).to.be.null
                expect(result).to.equal('response')
                expect(service.table.find).to.have.been.calledOnce
                expect(service.table.find).to.have.been.calledWith({ index: 'some index' })
                done()
            })
        })
        
        it('resolves with an error if table.find creates an error', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: {},
                table_cls: FakeTable
            })
            
            service.table.find = sinon.stub().rejects('some error')
            
            service.find({ data: { index: 'some index' } }, function (err, result)
            {
                expect(err.message).to.equal('some error')
                done()
            })
        })
        
        it('calls table.watch in feed mode', function (done)
        {
            var subscriber = { emit: sinon.stub() },
                cursor = { each: sinon.stub(), close: sinon.stub() },
                service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                })
            
            service.table.find = sinon.stub().resolves('initial value')
            service.table.watch = sinon.stub().resolves(cursor)
            
            service.find({ data: { index: 'some index' }, subscriber: subscriber }, function (err, result)
            {
                expect(err).to.be.null
                expect(service.table.find).to.have.been.calledOnce
                expect(service.table.find).to.have.been.calledWith({ index: 'some index' })
                expect(service.table.watch).to.have.been.calledOnce
                expect(service.table.watch).to.have.been.calledWith({ index: 'some index' })
                done()
            })
        })
        
        it('resolves with an error if table.watch creates an error', function (done)
        {
            var service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                }),
                subscriber = {
                    emit: sinon.stub()
                }
            
            service.table.find = sinon.stub().resolves('initial value')
            service.table.watch = sinon.stub().rejects('some error')
            
            service.find({ data: {}, subscriber: subscriber }, function (err, result)
            {
                expect(err.message).to.equal('some error')
                done()
            })
        })
        
        it('publishes changes to its subscriber', function (done)
        {
            var subscriber = { emit: sinon.stub() },
                cursor = { each: sinon.stub(), close: sinon.stub() },
                service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                })
            
            service.table.find = sinon.stub().resolves('initial value')
            service.table.watch = sinon.stub().resolves(cursor)
            
            service.find({ data: {}, subscriber: subscriber }, function (err, result)
            {
                expect(cursor.each).to.have.been.calledOnce
                expect(cursor.each.firstCall.args[0]).to.be.instanceof(Function)
                
                cursor.each.firstCall.args[0](null, 'some change')
                
                expect(subscriber.emit).to.have.been.calledTwice
                expect(subscriber.emit.firstCall.args).to.deep.equal(['initial', 'initial value'])
                expect(subscriber.emit.secondCall.args).to.deep.equal(['change', 'some change'])
                
                done()
            })
        })
        
        it('publishes errors to its subscriber', function (done)
        {
            var subscriber = { emit: sinon.stub() },
                cursor = { each: sinon.stub(), close: sinon.stub() },
                service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                })
            
            service.table.find = sinon.stub().resolves('initial value')
            service.table.watch = sinon.stub().resolves(cursor)
            
            service.find({ data: {}, subscriber: subscriber }, function (err, result)
            {
                expect(cursor.each).to.have.been.calledOnce
                expect(cursor.each.firstCall.args[0]).to.be.instanceof(Function)
                
                cursor.each.firstCall.args[0]({ message: 'some error' })
                
                expect(subscriber.emit).to.have.been.calledTwice
                expect(subscriber.emit.args[0].length).to.equal(2)
                expect(subscriber.emit.args[0][0]).to.equal('initial')
                expect(subscriber.emit.args[0][1]).to.equal('initial value')
                expect(subscriber.emit.args[1].length).to.equal(2)
                expect(subscriber.emit.args[1][0]).to.equal('error')
                expect(subscriber.emit.args[1][1].code).to.equal(500)
                expect(subscriber.emit.args[1][1].message).to.equal('some error')
                
                done()
            })
        })
    })
})