/**
 * Upload downloaded media to a S3 like service
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @version 1
 * @license MIT
 */

const fs = require('fs-extra')
const path = require('path')
const minio = require('triton-core/minio')
const proto = require('triton-core/proto')

module.exports = async (config, emitter, logger) => {
  const mediaProto = await proto.load('api.Media')
  const downloadStage = proto.stringToEnum(mediaProto, 'TelemetryStatusEntry', 'DOWNLOADING')
  return async job => {
    const { files, downloadPath } = job.lastStage

    const s3Client = minio.newClient(config)
    if (!Array.isArray(files)) {
      throw new Error(`Invalid files data type, expected array, got '${typeof files}'`)
    }

    logger.info('starting file upload')

    // create a bucket for this job
    const fileId = job.media.id
    if (!await s3Client.bucketExists('triton-staging')) {
      await s3Client.makeBucket('triton-staging')
    }

    let i = 0
    for (let file of files) {
      i++
      logger.info('upload', `${path.basename(file)}`)

      if (!await fs.pathExists(file)) {
        logger.error('failed to upload file, not found')
        throw new Error(`${file} not found.`)
      }

      const filename = Buffer.from(path.basename(file)).toString('base64')
      const objectName = path.join(fileId, 'original/', filename)
      await s3Client.fPutObject('triton-staging', objectName, file)

      // percent is calculated based on downloading taking up 50% of the time
      const percent = (i / files.length * 50) + 50

      // set status DOWNLOADING to percent
      await global.telem.emitProgress(fileId, downloadStage, Math.floor(percent))
    }

    // put a done object to ensure future downloaders don't try to re-download this
    await s3Client.putObject('triton-staging', path.join(fileId, 'original/', 'done'), 'true')

    logger.info('finished uploading all files')

    // cleanup
    try {
      await fs.remove(downloadPath)
    } catch (err) {
      logger.warn('err', `failed to clean up directory: ${err.message}`)
    }
  }
}
