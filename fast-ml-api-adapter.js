const NAMESPACE = 'fast-ml-api-adapter';

const Node = {
  crypto: require('crypto'),
  http: require('http'),
  process: process
};

const LEV = require('./log-event.js');

LEV.HOST = '197.242.94.138';
LEV.PORT = 4444;

const HOST = '0.0.0.0';
const PORT = 3000;

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

const TigerBeetle = {};

TigerBeetle.CREATE_TRANSFERS = { jobs: [], timestamp: 0, timeout: 0 };
TigerBeetle.ACCEPT_TRANSFERS = { jobs: [], timestamp: 0, timeout: 0 };

// Add an incoming prepare `payload`` to a batch of prepares.
// `callback` will be called once persisted to TigerBeetle.
TigerBeetle.create = function(payload, callback) {
  const self = this;
  self.push(self.CREATE_TRANSFERS, payload, callback);
};

// Add an incoming fulfill `payload`` to a batch of fulfills.
// `callback` will be called once persisted to TigerBeetle.
TigerBeetle.accept = function(payload, callback) {
  const self = this;
  self.push(self.ACCEPT_TRANSFERS, payload, callback);
};

TigerBeetle.push = function(batch, payload, callback) {
  const self = this;
  batch.jobs.push(new TigerBeetle.Job(payload, callback));
  if (batch.timeout === 0) {
    batch.timestamp = Date.now();
    batch.timeout = setTimeout(
      function() {
        self.execute(batch);
      },
      50 // Wait up to N ms for a batch to be collected...
    );
  } else if (batch.jobs.length > 200) {
    // ... and stop waiting as soon as we have at least N jobs ready to go.
    // This gives us a more consistent performance picture and lets us avoid
    // any non-multiple latency spikes from the timeout.
    clearTimeout(batch.timeout);
    self.execute(batch);
  }
};

TigerBeetle.execute = function(batch) {
  const ms = Date.now() - batch.timestamp;
  LEV(`${NAMESPACE}: batched ${batch.jobs.length} jobs in ${ms}ms`);
  // Cache reference to jobs array so we can reset the batch:
  const jobs = batch.jobs;
  // Reset the batch to start collecting a new batch:
  // We collect the new batch while commiting the previous batch to TigerBeetle.
  batch.jobs = [];
  batch.timestamp = 0;
  batch.timeout = 0;
  // Simulate 10ms network delay:
  setTimeout(
    function() {
      jobs.forEach(
        function(job) {
          job.callback();
        }
      );
    },
    10
  );
};

TigerBeetle.Job = function(payload, callback) {
  this.payload = payload;
  this.callback = callback;
};

function CreateServer() {
  const server = Node.http.createServer({},
    function(request, response) {
      const buffers = [];
      request.on('data', function(buffer) { buffers.push(buffer); });
      request.on('end',
        function() {
          if (request.url === '/transfers') {
            // Handle an incoming prepare:
            const buffer = Buffer.concat(buffers);
            const payload = JSON.parse(buffer.toString('ascii'));
            TigerBeetle.create(payload, function() {
              // Send prepare notification:
              PostNotification(PAYEE_HOST, PAYEE_PORT, '/transfers', buffer,
                function() {
                }
              );
              // ACK:
              response.statusCode = 202;
              response.end();
            });
          } else if (request.url.length > 36) {
            // Handle an incoming fulfill:
            const buffer = Buffer.concat(buffers);
            const payload = JSON.parse(buffer.toString('ascii'));
            TigerBeetle.accept(payload, function() {
              // Send fulfill notification:
              // We reuse the fulfill path as the notification path is the same.
              // We reuse the fulfill payload to avoid any GC complications.
              // This can always be optimized later without material overhead.
              PostNotification(PAYER_HOST, PAYER_PORT, request.url, buffer,
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
  maxSockets: 1000,
  maxFreeSockets: 1000,
  timeout: 60 * 1000
});

CreateServer();
