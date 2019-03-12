const nodeEnvFile = require('node-env-file')
const logger = require('@marcopeg/utils/lib/logger')

// Load local environment
nodeEnvFile('.env')
nodeEnvFile('.env.local')

// Init log level
logger.init()

module.exports = {}
