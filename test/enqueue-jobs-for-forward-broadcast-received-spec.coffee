redis      = require 'fakeredis'
Datastore  = require 'meshblu-core-datastore'
JobManager = require 'meshblu-core-job-manager'
mongojs    = require 'mongojs'
RedisNS    = require '@octoblu/redis-ns'
uuid       = require 'uuid'
{beforeEach, describe, it} = global
{expect}                   = require 'chai'
EnqueueJobsForForwardBroadcastReceived = require '../'

describe 'EnqueueJobsForForwardBroadcastReceived', ->
  beforeEach (done) ->
    database = mongojs 'meshblu-core-task-enqueue-jobs-for-forward-broadcast-received', ['devices']
    @datastore = new Datastore
      database: database
      collection: 'devices'

    database.devices.remove done

  beforeEach ->
    @redisKey = uuid.v1()
    @jobManager = new JobManager
      client: new RedisNS 'ns', redis.createClient(@redisKey)
      timeoutSeconds: 1

  beforeEach ->

    client = new RedisNS 'ns', redis.createClient(@redisKey)
    @sut = new EnqueueJobsForForwardBroadcastReceived {
      datastore:         @datastore
      jobManager:        new JobManager {client: client, timeoutSeconds: 1}
      uuidAliasResolver: {resolve: (uuid, callback) -> callback(null, uuid)}
    }

  describe '->do', ->
    describe 'with a device with no forward', ->
      beforeEach (done) ->
        @datastore.insert {
          uuid: 'subscriber'
        }, done

      describe 'when given a valid job', ->
        beforeEach (done) ->
          request =
            metadata:
              auth: {uuid: 'subscriber'}
              route: [{type: 'broadcast.received', from: 'subscriber', to: 'subscriber'}]
              responseId: 'its-electric'
            rawData: '{}'

          @sut.do request, (error, @response) => done error

        it 'should return a 204', ->
          expectedResponse =
            metadata:
              responseId: 'its-electric'
              code: 204
              status: 'No Content'

          expect(@response).to.deep.equal expectedResponse

    describe 'with a device with one forward', ->
      beforeEach (done) ->
        @datastore.insert {
          uuid: 'subscriber'
          meshblu:
            forwarders:
              broadcast:
                received: [{
                  type:     'meshblu'
                  emitType: 'broadcast.sent'
                }]
        }, done

      describe 'when given a valid job', ->
        beforeEach (done) ->
          request =
            metadata:
              auth: {uuid: 'subscriber'}
              route: [
                {from: 'emitter',    to: 'subscriber', type: 'broadcast.sent'}
                {from: 'subscriber', to: 'subscriber', type: 'broadcast.received'}
              ]
              responseId: 'its-electric'
            rawData: '{"devices":["*"],"payload":{"foo":"bar"}}'

          @sut.do request, (error, @response) => done error

        it 'should return a 204', ->
          expectedResponse =
            metadata:
              responseId: 'its-electric'
              code: 204
              status: 'No Content'

          expect(@response).to.deep.equal expectedResponse

        it 'should enqueue a job to deliver the message', (done) ->
          @jobManager.getRequest ['request'], (error, request) =>
            return done error if error?
            delete request?.metadata?.responseId
            expect(request).to.containSubset {
              metadata:
                jobType: 'DeliverBroadcastSent'
                auth:
                  uuid: 'subscriber'
                fromUuid: 'subscriber'
                toUuid: 'subscriber'
                forwardedRoutes: [
                  [
                    {from: 'emitter',    to: 'subscriber', type: 'broadcast.sent'}
                    {from: 'subscriber', to: 'subscriber', type: 'broadcast.received'}
                  ]
                ]
              rawData: '{"devices":["*"],"payload":{"foo":"bar"}}'
            }
            done()

      describe 'when given a valid job where the last hop from does not match the to', ->
        beforeEach (done) ->
          request =
            metadata:
              auth: {uuid: 'subscriber'}
              route: [{type: 'broadcast.received', from: 'emitter', to: 'subscriber'}]
              responseId: 'its-electric'
            rawData: '{}'

          @sut.do request, (error, @response) => done error

        it 'should return a 204', ->
          expectedResponse =
            metadata:
              responseId: 'its-electric'
              code: 204
              status: 'No Content'

          expect(@response).to.deep.equal expectedResponse

        it 'should not enqueue a job to deliver the message', (done) ->
          @jobManager.getRequest ['request'], (error, request) =>
            return done error if error?
            expect(request).not.to.exist
            done()

      describe 'when given a valid job with a forwardedRoutes', ->
        beforeEach (done) ->
          request =
            metadata:
              auth: {uuid: 'subscriber'}
              route: [
                {from: 'emitter',    to: 'subscriber', type: 'broadcast.received'}
                {from: 'subscriber', to: 'subscriber', type: 'broadcast.received'}
              ]
              forwardedRoutes: [
                [
                  {from: 'original', to: 'emitter', type: 'broadcast.sent'}
                ]
              ]
              responseId: 'its-electric'
            rawData: '{"devices":["*"],"payload":{"foo":"bar"}}'

          @sut.do request, (error, @response) => done error

        it 'should return a 204', ->
          expectedResponse =
            metadata:
              responseId: 'its-electric'
              code: 204
              status: 'No Content'

          expect(@response).to.deep.equal expectedResponse

        it 'should enqueue a job to deliver the message', (done) ->
          @jobManager.getRequest ['request'], (error, request) =>
            return done error if error?
            delete request?.metadata?.responseId
            expect(request).to.containSubset {
              metadata:
                jobType: 'DeliverBroadcastSent'
                auth:
                  uuid: 'subscriber'
                fromUuid: 'subscriber'
                toUuid: 'subscriber'
                forwardedRoutes: [
                  [
                    {from: 'original', to: 'emitter', type: 'broadcast.sent'}
                  ]
                  [
                    {from: 'emitter',    to: 'subscriber', type: 'broadcast.received'}
                    {from: 'subscriber', to: 'subscriber', type: 'broadcast.received'}
                  ]
                ]
              rawData: '{"devices":["*"],"payload":{"foo":"bar"}}'
            }
            done()

      describe 'when given a job with a this hop in the forwardedRoutes', ->
        beforeEach (done) ->
          request =
            metadata:
              auth: {uuid: 'subscriber'}
              route: [
                {from: 'subscriber', to: 'subscriber', type: 'broadcast.sent'}
                {from: 'subscriber', to: 'subscriber', type: 'broadcast.received'}
              ]
              forwardedRoutes: [
                [
                  {from: 'original', to: 'emitter', type: 'broadcast.sent'}
                ]
                [
                  {from: 'subscriber', to: 'subscriber', type: 'broadcast.sent'}
                  {from: 'subscriber', to: 'subscriber', type: 'broadcast.received'}
                ]
              ]
              responseId: 'its-electric'
            rawData: '{"devices":["*"],"payload":{"foo":"bar"}}'

          @sut.do request, (error, @response) => done error

        it 'should return a 204', ->
          expectedResponse =
            metadata:
              responseId: 'its-electric'
              code: 204
              status: 'No Content'

          expect(@response).to.deep.equal expectedResponse

        it 'should not enqueue a job to deliver the message', (done) ->
          @jobManager.getRequest ['request'], (error, request) =>
            return done error if error?
            expect(request).not.to.exist
            done()

    describe 'with a device with no forwards, but a webhook', ->
      beforeEach (done) ->
        @datastore.insert {
          uuid: 'subscriber'
          meshblu:
            forwarders:
              broadcast:
                received: [{
                  type:   'webhook'
                  url:    'example.com'
                  method: 'POST'
                }]
        }, done

      describe 'when given a valid job', ->
        beforeEach (done) ->
          request =
            metadata:
              auth: {uuid: 'subscriber'}
              route: [
                {from: 'emitter',    to: 'subscriber', type: 'broadcast.sent'}
                {from: 'subscriber', to: 'subscriber', type: 'broadcast.received'}
              ]
              responseId: 'its-electric'
            rawData: '{"devices":["*"],"payload":{"foo":"bar"}}'

          @sut.do request, (error, @response) => done error

        it 'should return a 204', ->
          expectedResponse =
            metadata:
              responseId: 'its-electric'
              code: 204
              status: 'No Content'

          expect(@response).to.deep.equal expectedResponse

        it 'should not enqueue a job to deliver the message', (done) ->
          @jobManager.getRequest ['request'], (error, request) =>
            return done error if error?
            expect(request).not.to.exist
            done()
