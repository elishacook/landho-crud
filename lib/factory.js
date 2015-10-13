'use strict'

module.exports = ReferenceCountingFactory


function ReferenceCountingFactory (options)
{
    this.items = {}
    this.create = options.create
    this.destroy = options.destroy
}

ReferenceCountingFactory.prototype.get = function (key, data, done)
{
    var item = this.items[key]
    if (!item)
    {
        this.create(key, data, function (err, item)
        {
            if (err)
            {
                done(err)
            }
            else
            {
                item.references = 1
                item.key = key
                this.items[key] = item
                done(null, item)
            }
        }.bind(this))
    }
    else
    {
        item.references++
        done(null, item)
    }
}

ReferenceCountingFactory.prototype.replace = function (item)
{
    item.references--
    
    if (item.references == 0)
    {
        this.destroy(item)
        delete this.items[item.key]
    }
}