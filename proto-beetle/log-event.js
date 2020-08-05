const Node = { http: require('http') };

let Events = [];
let Timeout = undefined;

function PostEvents() {
  const lines = Events.join('\n');
  Events = [];
  Timeout = undefined;
  const options = {
    method: 'POST',
    host: LogEvent.HOST,
    port: LogEvent.PORT,
    path: '/',
    headers: {}
  };
  const request = Node.http.request(options, function(response) {});
  request.on('error', function(error) {}); // Silence exceptions.
  request.write(lines);
  request.end();
}

function LogEvent(object) {
  if (!LogEvent.ENABLED) return;
  if (!LogEvent.HOST || !LogEvent.PORT) {
    throw new Error('LogEvent.HOST and PORT must be declared.');
  }
  if (typeof object === 'string') {
    const timestamp = Date.now();
    object = {
      start: timestamp,
      end: timestamp,
      label: object
    };
  }
  Events.push(JSON.stringify(object));
  if (Timeout === undefined) Timeout = setTimeout(PostEvents, 200);
}

LogEvent.ENABLED = true;
LogEvent.HOST = undefined;
LogEvent.PORT = undefined;

module.exports = LogEvent;
