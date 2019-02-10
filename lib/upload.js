/**
 * Upload downloaded media to a S3 like service
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @version 1
 * @license MIT
 */

const Minio = require('minio')
const fs = require('fs-extra')
const path = require('path')
const url = require('url')

const dyn = require('triton-core/dynamics')
const { Tags } = require('triton-core/tracer')

/**
 * Cleanup recursively cleans all items in a bucket
 * @param {Minio.Client} s3Client the s3client to use
 * @param {String} bucketId - the bucket to clean
 * @param {pino.Logger} logger - pino logger to use
 */
const cleanupBucket = async (s3Client, bucketId, logger) => {
  logger.info('cleaning up bucket', bucketId)
  return new Promise((resolve, reject) => {
    const objectNames = []
    const stream = s3Client.listObjects(bucketId, '', true)
    stream.on('data', async obj => {
      objectNames.push(obj.name)
    })
    stream.on('error', err => {
      return reject(err)
    })
    stream.on('end', async () => {
      try {
        await s3Client.removeObjects(bucketId, objectNames)
      } catch (err) {
        return reject(err)
      }

      return resolve()
    })
  })
}

module.exports = async (config, queue, emitter, logger, spanFactory) => {
  return async job => {
    const span = await spanFactory()
    const data = job.data
    const files = data.files
    const minioEndpoint = new url.URL(dyn('minio'))

    logger.info(`minio is at: '${minioEndpoint.hostname}' '${minioEndpoint.port}'`)
    const s3Client = new Minio.Client({
      // FIXME: not prod ready
      endPoint: minioEndpoint.hostname,
      port: parseInt(minioEndpoint.port, 10),
      useSSL: minioEndpoint.protocol === 'https',
      accessKey: config.keys.minio.accessKey,
      secretKey: config.keys.minio.secretKey
    })

    if (!Array.isArray(files)) {
      throw new Error(`Invalid files data type, expected array, got '${typeof files}'`)
    }

    span.setTag(Tags.CARD_ID, job.id)

    const updateInterval = setInterval(() => {
      logger.debug('updating job')
      job.active()
    }, 10000)

    logger.info('starting file upload')

    // create a bucket for this job
    logger.info('creating bucket for job id', job.id)
    if (await s3Client.bucketExists(job.id)) {
      await cleanupBucket(s3Client, job.id, logger)
      await s3Client.removeBucket(job.id)
    }
    await s3Client.makeBucket(job.id)

    for (let file of files) {
      logger.info('upload', `${path.basename(file)}`)

      if (!await fs.pathExists(file)) {
        logger.error('failed to upload file, not found')
        throw new Error(`${file} not found.`)
      }

      const fileStream = fs.createReadStream(file)
      const stat = await fs.stat(file)
      await s3Client.putObject(job.id, `original/${path.basename(file)}`, fileStream, stat.size)
      fileStream.close()
    }

    logger.info('finished uploading all files')
    clearInterval(updateInterval)
  }
}
