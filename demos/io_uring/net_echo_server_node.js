var net = require('net');
var server = net.createServer(function(socket) {
  socket.on('error', function(error) { console.log(error);});
  socket.on('end', function() { console.log('client disconnected'); });
  console.log('client connected');
  socket.pipe(socket);
});
server.listen(3001, '127.0.0.1',
  function() {
    console.log('net: echo server: node.js: listening on 127.0.0.1:3001...');
  }
);
