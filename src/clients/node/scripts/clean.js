const fs = require('fs')
const path = require('path')

rmdir(path.resolve(__dirname, '../build'));
rmdir(path.resolve(__dirname, '../dist'));
rmdir(path.resolve(__dirname, '../node_modules'));
rmdir(path.resolve(__dirname, '../src/zig-cache'));
rmdir(path.resolve(__dirname, '../zig'));

function rmdir(dir) {
    if (fs.existsSync(dir)) {
        for (const file of fs.readdirSync(dir)) {
            const current_path = path.join(dir, file);
            if (fs.lstatSync(current_path).isDirectory()) {
                rmdir(current_path);
            } else {
                fs.unlinkSync(current_path);
            }
        }
        fs.rmdirSync(dir);
    }
}
