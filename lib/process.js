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

const proto = require('triton-core/proto')

const mediaExts = [
  '.mp4',
  '.mkv',
  '.mov',
  '.webm'
]

/**
 * Find media files that are suitable to be processed / not discarded
 * @param {String} absPath path to media files
 * @param {Object} media media object
 * @param {pino.Logger} logger pino logger
 * @return {String[]} suitable media files
 */
const findMediaFiles = async (absPath, media, logger) => {
  const apiProto = await proto.load('api.Media')

  return new Promise((resolve, reject) => {
    const files = []
    klaw(absPath, {
      filter: item => {
        const processor = (filePath) => {
          const name = path.basename(filePath)
          if (fs.statSync(filePath).isDirectory()) {
            // in movie mode, assume the best
            if (media.type === proto.stringToEnum(apiProto, 'MediaType', 'MOVIE')) {
              return true
            }

            // explicitly skip folders that contain the name extra or commentary
            // at the start of a path
            if (/\/extras|\/commentary/i.test(item)) {
              return false
            }

            // allow folders with "season" or s\d+
            if (/s\d+|season/i.test(name)) {
              return true
            }
            return false
          }

          const ext = path.extname(name)
          if (mediaExts.indexOf(ext) !== -1) return true
          return false // filter non-media files.
        }

        // get the latest dir in the relative path
        const basePath = path.relative(absPath, item)
        const keep = processor(item)
        const type = fs.statSync(item).isDirectory() ? 'directory' : 'file'
        if (!keep) {
          logger.warn(`skipping ${type} '${basePath}'`)
        } else {
          logger.info(`including ${type} '${basePath}'`)
        }
        return keep
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

module.exports = async (config, emitter, logger) => {
  // determine if we need to process or not.
  return async job => {
    const file = job.lastStage

    logger.info('processing directory', file.path)

    const listOfFiles = await findMediaFiles(file.path, job.media, logger)
    if (listOfFiles.length === 0) {
      throw new Error('Failed to find any suitable media files')
    }

    logger.info('found', listOfFiles.length, 'media files')
    logger.info({
      files: listOfFiles
    })
    return {
      files: listOfFiles,
      downloadPath: file.path
    }
  }
}
