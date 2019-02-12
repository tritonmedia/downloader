/**
 * Upload downloaded media to a S3 like service
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @version 1
 * @license MIT
 */

const fs = require('fs-extra')
const path = require('path')

const { Tags } = require('triton-core/tracer')
const minio = require('triton-core/minio')

module.exports = async (config, queue, emitter, logger, spanFactory) => {
  return async job => {
    const span = await spanFactory()
    const data = job.data
    const files = data.files

    const s3Client = minio.newClient(config)
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
      const filename = Buffer.from(path.basename(file)).toString('base64')
      const objectName = path.join(fileId, 'original/', filename)
      await s3Client.fPutObject('triton-staging', objectName, file)
    }

    // put a done object to ensure future downloaders don't try to re-download this
    await s3Client.putObject('triton-staging', path.join(job.id, 'original/', 'done'), 'true')

    logger.info('finished uploading all files')
    clearInterval(updateInterval)
  }
}
