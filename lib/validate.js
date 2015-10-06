'use strict'

var validate = require('feelings')

module.exports = function (schema)
{
    return function (params, next)
    {
        var errors = validate(schema, params.data)
        if (errors)
        {
            next({
                code: 400,
                message: 'Validation error',
                errors: errors
            })
        }
        else
        {
            next()
        }
    }
}