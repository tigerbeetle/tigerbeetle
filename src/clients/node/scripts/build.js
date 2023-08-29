const { execSync } = require('child_process')

execSync('npm run build_tsc')
execSync('npm run build_lib')
