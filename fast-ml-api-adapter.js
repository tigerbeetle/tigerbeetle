const Node = {
  crypto: require('crypto'),
  http: require('http'),
  process: process
};

const LEV = require('./log-event.js');
const UUID4 = require('./uuid4.js');

LEV.HOST = '197.242.94.138';
LEV.PORT = 4444;

const NAMESPACE = 'fast-ml-api-adapter';

const HOST = '0.0.0.0';
const PORT = 3000; // TODO

// Preresolve TigerBeetle master IP address to avoid DNS overhead:
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
      response.statusCode = 203; // TODO Just to test we're hitting FMLAA.
      response.end();
    }
  );
  server.listen(PORT, HOST,
    function() {
      LEV(`${NAMESPACE}: Listening on ${HOST}:${PORT}...`);
    }
  );
}

function PostTransfer(transfer, end) {
  // var options = {
  //   method: 'POST',
  //   host: MOJALOOP_HOST,
  //   port: MOJALOOP_PORT,
  //   path: '/transfers',
  //   headers: transfer.headers
  // };
  // var request = Node.http.request(options,
  //   function(response) {
  //     var buffers = [];
  //     // TO DO: Error handling.
  //     response.on('data', function(buffer) { buffers.push(buffer); });
  //     response.on('end', function() {});
  //     Log(`POST ${transfer.uuid}: ${response.statusCode}`);
  //   }
  // );
  // request.write(transfer.body);
  // request.end();
}

CreateServer();
