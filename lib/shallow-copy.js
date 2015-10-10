'use strict'

module.exports = function (obj)
{
    var new_obj = {}
    Object.keys(obj).forEach(function (k)
    {
        new_obj[k] = obj[k]
    })
    return new_obj
}