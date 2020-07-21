const NAMESPACE = 'fast-ml-api-adapter';

const assert = require('assert');

const Node = {
  crypto: require('crypto'),
  http: require('http'),
  process: process
};

const LEV = require('./log-event.js');
const TigerBeetle = require('./client.js');

LEV.HOST = '197.242.94.138';
LEV.PORT = 4444;

const HOST = '0.0.0.0';
const PORT = 3000;

const LOCALHOST = '127.0.0.1';

// Preresolve TigerBeetle master IP address to avoid any DNS overhead:
const TIGER_BEETLE_HOST = '10.126.12.35';
const TIGER_BEETLE_PORT = 80;

// Test harness payee:
const PAYEE_HOST = '10.126.10.139';
const PAYEE_PORT = 3333;

// Test harness payer:
const PAYER_HOST = '10.126.10.139';
const PAYER_PORT = 7777;

Node.process.on('uncaughtException',
  function(error) {
    console.log(error);
    LEV(`${NAMESPACE}: UNCAUGHT EXCEPTION: ${error}`);
  }
);

// Measure event loop blocks of 10ms or more within fast-ml-api-adapter:
(function() {
  const delay = 5;
  let time = Date.now();
  setInterval(
    function() {
      const start = time + delay;
      const end = Date.now();
      const delta = end - start;
      if (delta > 10) {
        LEV({
          start: start,
          end: end,
          label: `${NAMESPACE}: event loop blocked for ${delta}ms`
        });
      }
      time = end;
    },
    delay
  );
})();


function CreateServer() {
  const server = Node.http.createServer({},
    function(request, response) {
      const buffers = [];
      request.on('data', function(buffer) { buffers.push(buffer); });
      request.on('end',
        function() {
          if (request.url === '/transfers') {
            // Handle an incoming prepare:
            const source = Buffer.concat(buffers);
            const object = JSON.parse(source.toString('ascii'));
            const target = TigerBeetle.encodeCreate(object);
            TigerBeetle.create(target, function() {
              // Send prepare notification:
              // We reuse the request path as the notification path is the same.
              // We reuse the request payload to avoid any GC complications.
              // This can always be optimized later without material overhead.
              PostNotification(PAYEE_HOST, PAYEE_PORT, request.url, source,
                function() {
                }
              );
              // ACK:
              response.statusCode = 202;
              response.end();
            });
          } else if (request.url.length > 36) {
            // TODO Improve request.url validation and parsing.
            // Handle an incoming fulfill:
            const id = request.url.split('/')[2];
            const source = Buffer.concat(buffers);
            const object = JSON.parse(source.toString('ascii'));
            const target = TigerBeetle.encodeAccept(id, object);
            TigerBeetle.accept(target, function() {
              // Send fulfill notification:
              PostNotification(PAYER_HOST, PAYER_PORT, request.url, source,
                function() {
                }
              );
              // ACK:
              response.statusCode = 202;
              response.end();
            });
          } else {
            console.log(`unknown request.url: ${request.url}`);
            response.end();
          }
        }
      );
    }
  );
  server.listen(PORT, HOST,
    function() {
      LEV(`${NAMESPACE}: Listening on ${HOST}:${PORT}...`);
    }
  );
}

function PostNotification(host, port, path, body, end) {
  const headers = {
    'Content-Length': body.length
  };
  const options = {
    agent: ConnectionPool,
    method: 'POST',
    host: host,
    port: port,
    path: path,
    headers: headers
  };
  const request = Node.http.request(options,
    function(response) {
      const buffers = [];
      response.on('data', function(buffer) { buffers.push(buffer); });
      response.on('end',
        function() {
          end();
        }
      );
    }
  );
  request.write(body);
  request.end();
}

// Create a keep-alive HTTP request connection pool:
// We don't want each and every notification to do a TCP handshake...
// This is critical. The lack of this causes multi-second event loop blocks.
const ConnectionPool = new Node.http.Agent({
  keepAlive: true,
  maxFreeSockets: 10000,
  timeout: 60 * 1000
});

CreateServer();

TigerBeetle.connect(TIGER_BEETLE_HOST, TIGER_BEETLE_PORT,
  function(error) {
    if (error) throw error;
    LEV(`${NAMESPACE}: Connected to Tiger Beetle...`);
  }
);
