const fs = require('fs')
const path = require('path')

const headers = require('node-api-headers')

// Compile a Set of all the symbols that could be exported
const allSymbols = new Set()
for (const ver of Object.values(headers.symbols)) {
    for (const sym of ver.node_api_symbols) {
        allSymbols.add(sym)
    }
    for (const sym of ver.js_native_api_symbols) {
        allSymbols.add(sym)
    }
}

// Write a 'def' file for NODE.EXE
const targetFile = path.resolve(__dirname, "../node.lib")
const allSymbolsArr = Array.from(allSymbols)
fs.writeFileSync(targetFile, 'NAME NODE.EXE\nEXPORTS\n' + allSymbolsArr.join('\n'))
