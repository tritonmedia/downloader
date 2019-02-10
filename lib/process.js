/**
 * Media post-processor. Determines if we need to convert or not.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const fs = require('fs-extra')
const path = require('path')
const klaw = require('klaw')

const { Tags } = require('triton-core/tracer')

const mediaExts = [
  '.mp4',
  '.mkv',
  '.mov',
  '.webm'
]

const findMediaFiles = async absPath => {
  return new Promise((resolve, reject) => {
    const files = []
    klaw(absPath, {
      filter: item => {
        if (fs.statSync(item).isDirectory()) {
          // Only process folders that seem like they might contain our content
          if (item.indexOf('Season')) {
            return true
          }

          return false
        }

        const ext = path.extname(item)
        if (mediaExts.indexOf(ext) !== -1) return true
        return false // filter non-media files.
      }
    })
      .on('data', item => {
        const stat = fs.statSync(item.path)
        if (stat.isDirectory()) return // skip
        files.push(item.path)
      })
      .on('end', () => {
        return resolve(files)
      })
      .on('error', err => {
        return reject(err)
      })
  })
}

module.exports = async (config, queue, emitter, logger, spanFactory) => {
  // determine if we need to process or not.
  return async job => {
    const span = await spanFactory()
    const file = job.data

    span.setTag(Tags.CARD_ID, job.id)
    logger.info('processing directory', file.path)

    const listOfFiles = await findMediaFiles(file.path)

    logger.info('found', listOfFiles.length, 'media files')
    span.setTag('media.files', listOfFiles.length)
    return {
      files: listOfFiles
    }
  }
}
