'use strict'

var landho = require('landho'),
    landho_crud = require('../lib/index'),
    api = null,
    crud = null

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
            
            service.table.create = sinon.stub().callsArgWith(1, null, 'a new foo')
            
            service.create({ data: { things: [] } }, function (err, result)
            {
                expect(err).to.be.null
                expect(result).to.equal('a new foo')
                expect(service.table.create).to.have.been.calledOnce
                expect(service.table.create).to.have.been.calledWith({ things: [] })
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
            
            service.table.create = sinon.stub().callsArgWith(1, 'AAAARGH!')
            
            service.create({ data: { things: [] }}, function (err, result)
            {
                expect(err).to.equal('AAAARGH!')
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
            
            service.table.update = sinon.stub().callsArgWith(1, null, 'an updated foo')
            
            service.update({ data: { id: '666', things: [] }}, function (err, result)
            {
                expect(err).to.be.null
                expect(result).to.equal('an updated foo')
                expect(service.table.update).to.have.been.calledOnce
                expect(service.table.update).to.have.been.calledWith({ id: '666', things: [] })
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
            
            service.table.update = sinon.stub().callsArgWith(1, 'AAAARGH!')
            
            service.update({ data: { id: '666', things: [] }}, function (err, result)
            {
                expect(err).to.equal('AAAARGH!')
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
        
        it('calls the table delete method when validation passes', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: {},
                table_cls: FakeTable
            })
            
            service.table.delete = sinon.stub().callsArgWith(1, null, 'an updated foo')
            
            service.delete({ data: { id: '666' }}, function (err, result)
            {
                expect(err).to.be.null
                expect(result).to.equal('an updated foo')
                expect(service.table.delete).to.have.been.calledOnce
                expect(service.table.delete).to.have.been.calledWith('666')
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
            
            service.table.delete = sinon.stub().callsArgWith(1, 'AAAARGH!')
            
            service.delete({ data: { id: '666' }}, function (err, result)
            {
                expect(err).to.equal('AAAARGH!')
                done()
            })
        })
    })
    
    describe('get()', function ()
    {
        it('calls table.get in simple mode', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: {},
                table_cls: FakeTable
            })
            
            service.table.get = sinon.stub().callsArgWith(1, null, 'some document')
            
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
            
            service.table.get = sinon.stub().callsArgWith(1, 'some error')
            
            service.get({ data: { id: '123' } }, function (err, result)
            {
                expect(err).to.equal('some error')
                done()
            })
        })
        
        it('calls table.get_and_watch in watch mode', function (done)
        {
            var subscriber = { emit: sinon.stub() },
                cursor = { each: sinon.stub(), close: sinon.stub() },
                service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                })
            
            service.table.get_and_watch = sinon.stub().callsArgWith(1, null, 'you got a channel')
            
            service.get({ data: { id: '123', watch: true }, socket: true }, function (err, result)
            {
                expect(err).to.be.null
                expect(service.table.get_and_watch).to.have.been.calledOnce
                expect(service.table.get_and_watch.args[0][0]).to.equal('123')
                done()
            })
        })
        
        it('resolves with an error if table.get_and_watch creates an error', function (done)
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
            
            service.table.get_and_watch = sinon.stub().callsArgWith(1, 'some error')
            
            service.get({ data: { id: '123', watch: true }, socket: true }, function (err, result)
            {
                expect(err).to.equal('some error')
                done()
            })
        })
        
        it('calls table.get_and_sync in watch mode', function (done)
        {
            var subscriber = { emit: sinon.stub() },
                cursor = { each: sinon.stub(), close: sinon.stub() },
                service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                })
            
            service.table.get_and_sync = sinon.stub().callsArgWith(1, null, 'you got a channel')
            
            service.get({ data: { id: '123', sync: true }, socket: true }, function (err, result)
            {
                expect(err).to.be.null
                expect(service.table.get_and_sync).to.have.been.calledOnce
                expect(service.table.get_and_sync.args[0][0]).to.equal('123')
                done()
            })
        })
        
        it('resolves with an error if table.get_and_sync creates an error', function (done)
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
            
            service.table.get_and_sync = sinon.stub().callsArgWith(1, 'some error')
            
            service.get({ data: { id: '123', sync: true }, socket: true }, function (err, result)
            {
                expect(err).to.equal('some error')
                done()
            })
        })
    })
    
    describe('find()', function ()
    {
        it('calls table.find in simple mode', function (done)
        {
            var service = crud(
            {
                name: 'foos',
                schema: {},
                table_cls: FakeTable
            })
            
            service.table.find = sinon.stub().callsArgWith(1, null, 'response')
            
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
            
            service.table.find = sinon.stub().callsArgWith(1, 'some error')
            
            service.find({ data: { index: 'some index' } }, function (err, result)
            {
                expect(err).to.equal('some error')
                done()
            })
        })
        
        it('calls table.find_and_watch in watch mode', function (done)
        {
            var service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                })
            
            service.table.find_and_watch = sinon.stub().callsArgWith(1, null, 'result')
            
            service.find({ data: { index: 'some index', watch: true }, socket: true }, function (err, result)
            {
                expect(err).to.be.null
                expect(service.table.find_and_watch).to.have.been.calledOnce
                expect(service.table.find_and_watch).to.have.been.calledWith({ index: 'some index', watch: true })
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
                })
            
            service.table.find_and_watch = sinon.stub().callsArgWith(1, 'some error')
            
            service.find({ data: { index: 'some index', watch: true }, socket: true }, function (err, result)
            {
                expect(err).to.equal('some error')
                done()
            })
        })
        
        it('calls table.find_and_sync in sync mode', function (done)
        {
            var service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                })
            
            service.table.find_and_sync = sinon.stub().callsArgWith(1, null, 'result')
            
            service.find({ data: { index: 'some index', sync: true }, socket: true }, function (err, result)
            {
                expect(err).to.be.null
                expect(service.table.find_and_sync).to.have.been.calledOnce
                expect(service.table.find_and_sync).to.have.been.calledWith({ index: 'some index', sync: true })
                done()
            })
        })
        
        it('resolves with an error if table.sync creates an error', function (done)
        {
            var service = crud(
                {
                    name: 'foos',
                    schema: {},
                    table_cls: FakeTable
                })
            
            service.table.find_and_sync = sinon.stub().callsArgWith(1, 'some error')
            
            service.find({ data: { index: 'some index', sync: true }, socket: true }, function (err, result)
            {
                expect(err).to.equal('some error')
                done()
            })
        })
    })
})