'use strict'

var db = require('./db'),
    service = require('./service')

module.exports = function (api, config)
{
    var crud = function (description)
    {
        return service(api, description)
    }
    
    crud.db = db
    
    crud.init = function ()
    {
        return db.init(config)
    }
    
    return crud
}