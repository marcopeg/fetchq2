const nodeEnvFile = require('node-env-file')

// Load local environment
nodeEnvFile('.env')
nodeEnvFile('.env.local')

module.exports = {}
