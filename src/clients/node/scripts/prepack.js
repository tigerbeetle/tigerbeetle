const fs = require('fs')
const path = require('path')
const { execSync } = require('child_process')

execSync('npm run build')
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
