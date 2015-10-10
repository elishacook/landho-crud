'use strict'

var chai = require('chai'),
    sinon = require('sinon'),
    sinon_chai = require('sinon-chai'),
    Bluebird = require('bluebird')

require('sinon-as-promised')(Bluebird)

chai.use(sinon_chai)
global.expect = chai.expect
global.sinon = sinon