/**
 * Main Media Processor
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @version 1
 */

const _ = require('lodash')
const EventEmitter = require('events').EventEmitter
const path = require('path')

const AMQP = require('triton-core/amqp')
const proto = require('triton-core/proto')
const dyn = require('triton-core/dynamics')
const minio = require('triton-core/minio')
const Telemetry = require('triton-core/telemetry')

/* eslint no-unused-vars: 1 */
const { opentracing, Tags, unserialize, error, serialize } = require('triton-core/tracer')
const os = require('os')
const logger = require('pino')({
  name: path.basename(__filename)
})

const EmitterTable = {}

const stages = [
  'download',
  'process',
  'upload'
]

const activeJobs = []
/**
 * Main function that builds the execution stages
 * @param {Object} config - config object
 * @param {opentracing.Tracer} tracer - tracer object
 */
module.exports = async (config, tracer) => {
  const s3Client = minio.newClient(config)

  const amqp = new AMQP(dyn('rabbitmq'), 1)
  await amqp.connect()

  const telem = new Telemetry(dyn('rabbitmq'))
  await telem.connect()

  const downloadProto = await proto.load('api.Download')
  const convertProto = await proto.load('api.Convert')

  /**
   * Process new media
   *
   */
  const processor = async rmsg => {
    const msg = proto.decode(downloadProto, rmsg.message.content)
    const fileId = msg.media.creatorId
    const jobId = msg.media.id

    // set DOWNLOADING status
    await telem.emitStatus(jobId, 1)

    let type = 'tv'

    activeJobs.push({
      cardId: fileId,
      jobId
    })

    const loggerData = {
      jobId,
      type,
      fileId
    }
    const child = logger.child(loggerData)

    const emitter = EmitterTable[fileId] = new EventEmitter()

    const staticData = {
      id: fileId,
      type
    }

    const stageStorage = {}
    const stageTable = {}
    const stage = {
      fn: function () {
        throw new Error('Invalid stage function, never overriden')
      }
    }

    // callback system to keep scope
    let lastStageData = {}

    // dynamically generate our stages
    for (const stage of stages) {
      logger.debug('creating stage', stage)

      const modulePath = path.join(__dirname, `${stage}.js`)
      const fn = await require(modulePath)(config, emitter, logger.child(_.extend({
        name: path.basename(modulePath)
      }, loggerData)))

      if (typeof fn !== 'function') {
        const err = new Error(`Invalid stage '${stage}' return value was not a function`)
        throw err
      }

      stageTable[stage] = {
        fn: fn
      }
    }

    let failed = false
    try {
      logger.info('checking s3 bucket to see if files already exist for id', jobId)
      await s3Client.getObject('triton-staging', path.join(jobId, 'original/', 'done'))
    } catch (err) {
      logger.info('failed to find done file in staging', err)
      failed = true
    }

    if (failed) { // start the downloader
      logger.info('starting main processor after successful stage init')
      try {
        for (let stage of stages) {
          // TODO: make safer
          const staticCopy = _.create(msg, {
            lastStage: lastStageData
          })

          logger.info(`invoking stage '${stage}'`)
          stageStorage[stage] = {}
          const data = await stageTable[stage].fn(staticCopy)
          lastStageData = data
          emitter.emit('progress', 0)
        }
      } catch (err) {
        child.error('failed to invoke stage:', err.message)
        return
      }
      logger.info('creating convert job')
    } else {
      logger.warn('skipping download due to files existing in triton-staging')
    }

    const payload = {
      createdAt: new Date().toISOString(),
      media: msg.media
    }

    try {
      const encoded = proto.encode(convertProto, payload)
      await amqp.publish('v1.convert', encoded)
    } catch (err) {
      return logger.error('failed to create job:', err.message || err)
    }
    rmsg.ack()
  }

  amqp.listen('v1.download', processor)

  const app = require('express')()

  app.get('/health', (req, res) => {
    return res.status(200).send({
      ping: 'pong'
    })
  })

  app.listen(process.env.PORT || 3401)
  logger.info('successfully connected to queue and started server')

  return async () => {
    if (activeJobs.length === 0) process.exit(0)

    await amqp.close()

    // TODO: fix this to work like it did before
    process.exit(1)
  }
}
