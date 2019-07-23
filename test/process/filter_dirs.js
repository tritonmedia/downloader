/* eslint-env mocha */
const Processor = require('../../lib/process')
const path = require('path')
const proto = require('triton-core/proto')
const assert = require('assert')
const logger = require('pino')({
  name: path.basename(__filename)
})

const mockLogger = {
  info: () => {},
  warn: () => {},
  error: () => {}
}

let processor
describe('process()', () => {
  beforeEach(async () => {
    processor = await Processor({}, {}, process.env.USE_REAL_LOGGER ? logger : mockLogger)
  })

  it('should filter non season directories', async () => {
    const mediaProto = await proto.load('api.Media')

    const mediaObj = {
      id: '<uuid>',
      type: proto.stringToEnum(mediaProto, 'MediaType', 'TV')
    }

    const baseDir = 'filter_dirs/should_filter_non_season_directories'
    const res = await processor({
      lastStage: {
        // Test directory
        path: path.join(__dirname, baseDir)
      },
      media: mediaObj
    })

    assert.strictEqual(res.files.length, 2)
    assert.strictEqual(res.files[0], path.join(__dirname, baseDir, 'S1/KonoSuba S1E1.mkv'))
  })

  it('should read all dirs when processing a movie', async () => {
    const mediaProto = await proto.load('api.Media')

    const mediaObj = {
      id: '<uuid>',
      type: proto.stringToEnum(mediaProto, 'MediaType', 'MOVIE')
    }

    const baseDir = 'filter_dirs/should_read_all_dirs_when_processing_a_movie'
    const res = await processor({
      lastStage: {
        // Test directory
        path: path.join(__dirname, baseDir)
      },
      media: mediaObj
    })
    assert.strictEqual(res.files.length, 1)
    assert.strictEqual(res.files[0], path.join(__dirname, baseDir, 'Some Movie Dir/Your Name.mkv'))
  })
})
