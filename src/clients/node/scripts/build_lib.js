const fs = require('fs')
const os = require('os')
const path = require('path')
const headers = require('node-api-headers')
const { execSync } = require('child_process')

const isWindows = os.platform() === 'win32'
const zig = path.resolve('../../../zig/zig' + (isWindows ? '.exe' : ''))

// Compile a Set of all the symbols that could be exported.
const allSymbols = new Set()
for (const ver of Object.values(headers.symbols)) {
    for (const sym of ver.node_api_symbols) {
        allSymbols.add(sym)
    }
    for (const sym of ver.js_native_api_symbols) {
        allSymbols.add(sym)
    }
}

// Write a '.def' file for node.dll.
const defFile = path.resolve(__dirname, '../node.def')
const libFile = path.resolve(__dirname, '../node.lib')
const allSymbolsArr = Array.from(allSymbols)
fs.writeFileSync(defFile, 'EXPORTS\n    ' + allSymbolsArr.join('\n    '))

// Compile '.def' file to '.lib' file for zig build node_client to link to.
execSync(`${zig} dlltool -m i386:x86-64 -D node.exe -d ${defFile} -l ${libFile}`)

// Build the tigerbeetle node client.
process.chdir(path.resolve('../../../'))
execSync(`${zig} build node_client -Doptimize=ReleaseSafe -Dconfig=production`)
