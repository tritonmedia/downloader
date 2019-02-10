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
      useSSL: minioEndpoint.protocol === 'https:',
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
    if (!await s3Client.bucketExists('triton-staging')) {
      await s3Client.makeBucket('triton-staging')
    }

    for (let file of files) {
      logger.info('upload', `${path.basename(file)}`)

      if (!await fs.pathExists(file)) {
        logger.error('failed to upload file, not found')
        throw new Error(`${file} not found.`)
      }

      const fileId = job.id
      const fileStream = fs.createReadStream(file)
      const stat = await fs.stat(file)
      const filename = Buffer.from(path.basename(file)).toString('base64')
      const objectPath = path.join(fileId, 'original/', filename)
      await s3Client.putObject('triton-staging', objectPath, fileStream, stat.size)
      fileStream.close()
    }

    logger.info('finished uploading all files')
    clearInterval(updateInterval)
  }
}
