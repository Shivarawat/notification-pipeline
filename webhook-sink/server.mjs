// Stand-in for a customer's webhook endpoint. Fails a share of requests on purpose
// so retries and the dead-letter topic can be exercised.
//   FAILURE_RATE=0.2  -> 20% of requests get HTTP 503
//   LATENCY_MS=20     -> each request waits 20 ms
import http from 'node:http';

const PORT = Number(process.env.PORT || 8080);
const FAILURE_RATE = Number(process.env.FAILURE_RATE || 0);
const LATENCY_MS = Number(process.env.LATENCY_MS || 0);
const stats = { received: 0, succeeded: 0, failed: 0 };

http
  .createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/stats') {
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(stats));
      return;
    }
    if (req.method !== 'POST' || req.url !== '/webhook') {
      res.writeHead(404).end();
      return;
    }
    req.resume();
    req.on('end', () => {
      stats.received++;
      setTimeout(() => {
        if (Math.random() < FAILURE_RATE) {
          stats.failed++;
          res.writeHead(503).end();
        } else {
          stats.succeeded++;
          res.writeHead(200).end();
        }
      }, LATENCY_MS);
    });
  })
  .listen(PORT, () => console.log(JSON.stringify({ event: 'webhook_sink_started', port: PORT, FAILURE_RATE, LATENCY_MS })));
