/**
 * Download new media.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const Webtorrent = require('webtorrent')
const request = require('request-promise-native')
const path = require('path')
const fs = require('fs-extra')
const url = require('url')
const uri2path = require('file-uri-to-path')

const { Minio, getObjects } = require('triton-core/minio')

const client = new Webtorrent()

const TIMEOUT = 240000

// main function
module.exports = async (config, emitter, logger) => {
  const methods = {
    /**
     * Download via torrent.
     *
     * @param {String} magnet          magnet link
     * @param {String} id              File ID
     * @param {String} downloadPath    Path to save file(s) in
     * @param {Object} job             Job Object
     * @return {Promise}               You know what to do.
     */
    magnet: async (magnet, id, downloadPath, job) => {
      logger.info('url', magnet.substr(0, 25) + '...')

      return new Promise((resolve, reject) => {
        const initStallHandler = setTimeout(() => {
          logger.warn('download failed to progress, killing')
          reject(new Error('Metadata fetch stalled'))
        }, TIMEOUT) // 2 minutes

        client.add(magnet, {
          path: downloadPath
        }, torrent => {
          const hash = torrent.infoHash

          logger.debug('hash', hash)
          logger.debug('files', torrent.files.length)

          clearTimeout(initStallHandler)
          logger.debug('cleared timeout handler')

          // sadness
          let lastProgress, stallHandler, progress
          const downloadProgress = setInterval(async () => {
            progress = torrent.progress * 100
            logger.info('download progress', progress)

            // set status DOWNLOADING to progress / 2 (assume download time)
            await global.telem.emitProgress(id, 1, progress / 2)
          }, 1000 * 30) // every 30 seconds, emit download stats

          stallHandler = setInterval(() => {
            logger.info('stall check', progress, lastProgress)

            if (progress === lastProgress) {
              clearInterval(downloadProgress)
              const err = new Error('Download stalled.')
              err.code = 'ERRDLSTALL'
              return reject(err)
            }

            lastProgress = progress
          }, TIMEOUT)

          torrent.on('error', err => {
            logger.error('torrent error')
            console.log(err)
            client.remove(hash)
            return reject(err)
          })

          torrent.on('done', () => {
            logger.debug('finished, clearing watchers')

            clearInterval(downloadProgress)
            clearInterval(stallHandler)

            client.remove(hash)

            return resolve()
          })
        })
      })
    },

    /**
     * Download via HTTP.
     *
     * @param  {String} resourceUrl   resource url
     * @param  {String} id            File ID
     * @param  {String} downloadPath  Path to download file too
     * @param  {Object} job           Job Object
     * @return {Promise}              .then/.catch etc
     */
    http: async (resourceUrl, id, downloadPath, job) => {
      logger.info('http', resourceUrl)

      return new Promise(async (resolve, reject) => {
        const parsed = new url.URL(resourceUrl)
        const filename = path.basename(parsed.pathname)
        const output = path.join(downloadPath, filename)

        const fileType = path.parse(parsed.pathname)
        if (fileType.ext === '.torrent') {
          logger.info('downloading a .torrent, chaining to torrent downloader')

          let resp
          try {
            resp = await methods['magnet'](resourceUrl, id, downloadPath, job)
          } catch (err) {
            return reject(err)
          }

          return resolve(resp)
        }

        await fs.ensureDir(downloadPath)

        const write = fs.createWriteStream(output)
        request(resourceUrl).pipe(write)

        // assume it's downloadProgress
        write.on('close', () => {
          return resolve()
        })
      })
    },

    /**
     * Download via file URL
     *
     * @param {String} resourceUrl - resource url
     * @param {String} id - card id
     * @param {String} downloadPath - path to download too
     * @param {Object} job - job object
     */
    file: async (resourceUrl, id, downloadPath, job) => {
      if (process.env.ALLOW_FILE_URLS !== 'true') {
        throw new Error('File URLs are not allowed.')
      }

      const qualifiedPath = uri2path(resourceUrl)
      const parsed = path.parse(qualifiedPath)
      const output = path.join(downloadPath, `${parsed.name}${parsed.ext}`)

      logger.debug('file', qualifiedPath, '->', output)

      return fs.copyFile(qualifiedPath, output)
    },

    /**
     * S3 Download Files from a S3 bucket
     *
     * @param {String} resourceURL - resource URL (expected to be bucket://endpoint,bucket_name,accessKey,secretKey,subFolder).
     * @param {String} id - card id
     * @param {String} downloadPath - path to download to
     * @param {Object} job - job object
     */
    bucket: async (resourceURL, id, downloadPath, job) => {
      logger.info('bucket', resourceURL)
      const params = resourceURL.split(',')

      const endpoint = params[0].replace('bucket://', '')
      const bucketName = params[1]
      const accessKey = params[2]
      const secretKey = params[3]
      const subFolder = params[4]

      logger.info('bucket', `using s3 endpoint: ${endpoint}`)
      const client = new Minio.Client({
        endPoint: endpoint,
        useSSL: true,
        accessKey,
        secretKey
      })

      const items = await getObjects(client, bucketName, subFolder.replace(/\/$/, '') + '/')
      for (const item of items) {
        if (!item.name) {
          continue
        }
        // remove leading directory by removing the subFolder path.
        const fileDownloadPath = path.join(downloadPath, item.name.replace(subFolder, ''))
        logger.info(`Downloading file '${item.name}' from bucket '${bucketName}' to '${fileDownloadPath}'}`)
        await client.fGetObject(bucketName, item.name, fileDownloadPath)
      }
    }
  }

  return async job => {
    const media = job.media
    const fileId = job.media.id

    let pathPrefix = ''
    if (!path.isAbsolute(config.instance.download_path)) {
      logger.debug('converting not absolute path to absolute path')
      pathPrefix = path.join(__dirname, '..')
    }

    const downloadPath = path.join(pathPrefix, config.instance.download_path, fileId)

    const url = media.sourceURI
    let protocol
    switch (media.source) {
      case 0:
        protocol = 'http'
        break
      case 1:
        protocol = 'magnet'
        break
      case 2:
        protocol = 'file'
        break
      default:
        logger.error('unknown protocol', job.media.source)
        return
    }

    try {
      await fs.ensureDir(downloadPath)
      logger.info('created downloadPath', downloadPath)
    } catch (e) {
      logger.error('Failed to create directory', e.message)
    }

    logger.info(`Trying to download with protocol '${protocol}', the URL '${url}'`)

    // set status downloading progress to 0
    await global.telem.emitProgress(fileId, 1, 0)
    const method = methods[protocol]
    if (!method) {
      const err = new Error('Protocol not supported.')
      throw err
    }

    try {
      await method(url, fileId, downloadPath, job)
    } catch (err) {
      logger.error('Download error: ', err.message)
      throw err
    }

    logger.info('finished download')

    // set status DOWNLOADING to progress 50
    await global.telem.emitProgress(fileId, 1, 50)
    return {
      path: downloadPath
    }
  }
}
