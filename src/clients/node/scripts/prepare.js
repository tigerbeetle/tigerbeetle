const fs = require('fs')
const headers = require('node-api-headers')
const os = require('os')
const path = require('path')
const { execSync } = require('child_process')

execSync('node ./node_modules/typescript/bin/tsc')

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
execSync(`${zig} build node_client -Drelease -Dconfig=production`)

const bin_path = path.resolve(__dirname, '../dist/bin');
const libs = get_libs(bin_path);

for (const lib of libs) {
    fs.renameSync(lib , path.join(path.dirname(lib), 'client.node'))
}

function get_libs(dir) {
    const entries = fs.readdirSync(dir);
    const files = [];

    for (const entry of entries) {
        const full_path = path.join(dir, entry);
        const stats = fs.statSync(full_path);

        if (stats.isFile()) {
            const extname = path.extname(entry);
            if (extname === '.so' || extname === '.dylib' || extname === '.dll') {
                files.push(full_path);
            }
        } else if (stats.isDirectory()) {
            const subFiles = get_libs(full_path);
            files.push(...subFiles);
        }
    }

    return files;
}
