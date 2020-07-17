var Node = {
  crypto: require('crypto')
};

const TABLE = [];
for (var index = 0; index < 256; index++) {
  TABLE[index] = (index + 0x100).toString(16).substr(1);
}

function UUID4() {
  // TO DO: Optimize by replacing randomBytes() with AES stream cipher.
  var buffer = Node.crypto.randomBytes(16);
  buffer[6] = (buffer[6] & 0x0f) | 0x40;
  buffer[8] = (buffer[8] & 0x3f) | 0x80;
  return [
    TABLE[buffer[ 0]],
    TABLE[buffer[ 1]],
    TABLE[buffer[ 2]],
    TABLE[buffer[ 3]],
    '-',
    TABLE[buffer[ 4]],
    TABLE[buffer[ 5]],
    '-',
    TABLE[buffer[ 6]],
    TABLE[buffer[ 7]],
    '-',
    TABLE[buffer[ 8]],
    TABLE[buffer[ 9]],
    '-',
    TABLE[buffer[10]],
    TABLE[buffer[11]],
    TABLE[buffer[12]],
    TABLE[buffer[13]],
    TABLE[buffer[14]],
    TABLE[buffer[15]],
  ].join('');
}

module.exports = UUID4;
