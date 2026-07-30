// MINIMAL TEST SERVER — remove this after debugging
const http = require('http');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    ok: true,
    message: 'Minimal server is running',
    port: PORT,
    nodeVersion: process.version,
    url: req.url,
    envKeys: Object.keys(process.env).sort(),
    timestamp: new Date().toISOString(),
  }));
});

server.listen(PORT, () => {
  console.log(`Minimal test server running on port ${PORT}`);
});

