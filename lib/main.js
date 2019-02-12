/**
 * Main Media Processor
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @version 1
 */

const _ = require('lodash')
const EventEmitter = require('events').EventEmitter
const async = require('async')
const path = require('path')
const kue = require('kue')
const Redis = require('ioredis')
const dyn = require('triton-core/dynamics')
const minio = require('triton-core/minio')
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

const metricsDb = dyn('redis') + '/1'
logger.info('metrics is at', metricsDb)
const metrics = new Redis(metricsDb)
const activeJobs = []

/**
 * Main function that builds the execution stages
 * @param {Object} config - config object
 * @param {kue.Queue} queue - queue object
 * @param {opentracing.Tracer} tracer - tracer object
 */
module.exports = async (config, queue, tracer) => {
  /**
   * Process new media
   *
   * @param {kue.Job} container the job
   * @param {function} done callback
   */
  const processor = async (container, realDone) => {
    const data = container.data
    const media = data.media
    const fileId = data.id
    const rawRootContext = data.rootContext

    const rootContext = unserialize(rawRootContext)
    const span = tracer.startSpan('stageProcessor', {
      references: [ opentracing.followsFrom(rootContext) ]
    })

    span.setTag(Tags.CARD_ID, data.id)

    let type = 'tv'

    activeJobs.push({
      cardId: fileId,
      jobId: container.id
    })

    const movieLabel = _.find(data.card.labels, {
      name: 'Movie'
    })
    if (movieLabel) type = 'movie'

    span.setTag(Tags.MEDIA_TYPE, type)

    const loggerData = {
      job: container.id,
      type,
      fileID: fileId,
      attempt: container._attempts || 1
    }
    const child = logger.child(loggerData)

    const emitter = EmitterTable[fileId] = new EventEmitter()

    const staticData = {
      id: fileId,
      card: data.card,
      media,
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
    let lastTrace = span

    const done = err => {
      if (err) {
        error(lastTrace, err)
        logger.error(err)
        return realDone(err)
      }

      if (!span.finished) span.finish()
      return realDone()
    }

    // dynamically generate our stages
    try {
      async.forEach(stages, async stage => {
        logger.debug('creating stage', stage)

        // generate the span when called to prevent issues with timing
        const spanFactory = async () => {
          logger.info('stage', stage, 'generating span')
          if (!lastTrace.finished) {
            lastTrace.finish()
            lastTrace.finished = true
          }
          lastTrace = tracer.startSpan(stage, {
            references: [
              opentracing.followsFrom(span.context())
            ]
          })
          return lastTrace
        }

        const modulePath = path.join(__dirname, `${stage}.js`)

        // quick compat wrapper
        const fn = await require(modulePath)(config, queue, emitter, logger.child(_.extend({
          name: path.basename(modulePath)
        }, loggerData)), spanFactory)

        if (typeof fn !== 'function') {
          const err = new Error(`Invalid stage '${stage}' return value was not a function`)
          throw err
        }

        stageTable[stage] = _.extend({
          fn: fn
        }, stage)
      }, async err => {
        if (err) {
          return done(err)
        }

        // kick off the queue
        logger.info('starting main processor after successful stage init')
        try {
          let exists = true
          const s3Client = minio.newClient(config)
          try {
            await s3Client.statObject('triton-staging', path.join(fileId, 'original/', 'done'))
          } catch (err) {
            exists = false
          }

          // TODO: don't be like this
          if (!exists) {
            for (let stage of stages) {
              // TODO: make safer
              const staticCopy = _.create(staticData, {
                data: lastStageData,
                active: () => {
                  container.state('active')
                  container.set('updated_at', Date.now())
                  container.refreshTtl()
                }
              })

              logger.info(`invoking stage '${stage}'`)
              stageStorage[stage] = {}
              const data = await stageTable[stage].fn(staticCopy)
              lastStageData = data
              emitter.emit('progress', 0)
            }
          } else {
            logger.info('skipping download process for job due to done file existing in bucket')
          }
        } catch (err) {
          child.error('failed to invoke stage:', err.message)
          console.log(err)

          const errorMetrics = {
            job: fileId,
            stage: stage,
            host: os.hostname(),
            data: {
              message: 'Internal Server Error',
              code: 'ERRNOCODE'
            }
          }

          if (data.data instanceof Error) {
            errorMetrics.data = {
              message: data.data.message,
              code: data.data.code
            }
          }

          metrics.publish('error', JSON.stringify(errorMetrics))
          return done(data.data)
        }

        logger.info('creating convert job')

        const newData = container
        newData.data.rootContext = serialize(lastTrace)
        console.log(newData.data)
        queue.create('convert', newData.data)
          .attempts(3)
          .save(err => {
            if (err) {
              return error(lastTrace, new Error('Failed to save job'))
            }

            logger.info('convert job created')

            return done()
          })
      })
    } catch (err) {
      return done(err)
    }
  }
  queue.process('newMedia', 1, processor)

  const app = require('express')()

  app.get('/health', (req, res) => {
    return res.status(200).send({
      ping: 'pong'
    })
  })

  app.listen(process.env.PORT || 3401)
  logger.info('successfully connected to queue and started server')

  /**
   * getJob returns a kue job
   * @param {String} id jobId
   * @returns {kue.Job} job
   */
  const getJob = id => {
    return new Promise((resolve) => {
      kue.Job.get(id, (err, job) => {
        if (err) return logger.warn(`failed to remove job ${id}: ${err.message}`)
        return resolve(job)
      })
    })
  }

  return async () => {
    if (activeJobs.length === 0) process.exit(0)

    for (let container of activeJobs) {
      const { jobId } = container
      logger.info(`marking job ${jobId} as failed`)
      const job = await getJob(jobId)
      await job.error(new Error('SIGINT'))
    }

    // TODO: fix this to work like it did before
    process.exit(1)
  }
}
