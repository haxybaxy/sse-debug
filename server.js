// Minimal SSE test server (no dependencies). Run: node server.js
const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = process.env.PORT || 3000;
const WORDS = "Hello! This is a simulated AI chat response streamed token by token over server-sent events so you can watch it arrive live in the browser devtools.".split(" ");

function streamTokens(req, res, { delay, buffered, label }) {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no", // disables buffering in nginx if you ever put one in front
  });

  const chunks = [];
  const send = (event, data, id) => {
    const msg = `id: ${id}\nevent: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    if (buffered) chunks.push(msg);
    else res.write(msg);
  };

  if (!buffered) res.write(": stream opened\n\n"); // comment line flushes headers immediately

  let i = 0;
  const timer = setInterval(() => {
    if (i < WORDS.length) {
      send("token", { i, text: WORDS[i] + " ", sentAt: Date.now() }, i);
      i++;
    } else {
      clearInterval(timer);
      send("done", { total: WORDS.length, sentAt: Date.now() }, i);
      if (buffered) res.write(chunks.join(""));
      res.end();
      console.log(`[${label}] finished (${buffered ? "buffered" : "live"})`);
    }
  }, delay);

  res.on("close", () => clearInterval(timer)); // res, not req: req "close" fires once a POST body is read
  console.log(`[${label}] started (${buffered ? "buffered" : "live"}, ${delay}ms/token)`);
}

http
  .createServer((req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const delay = Number(url.searchParams.get("delay")) || 300;
    const buffered = url.searchParams.get("buffered") === "1";

    if (url.pathname === "/") {
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      return fs.createReadStream(path.join(__dirname, "index.html")).pipe(res);
    }
    // EventSource-style: GET
    if (url.pathname === "/sse" && req.method === "GET") {
      return streamTokens(req, res, { delay, buffered, label: "GET /sse" });
    }
    // AI-chat-style: POST with a JSON body, read via fetch() + ReadableStream
    if (url.pathname === "/chat" && req.method === "POST") {
      req.resume(); // drain body; we don't need it
      return streamTokens(req, res, { delay, buffered, label: "POST /chat" });
    }
    res.writeHead(404).end("not found");
  })
  .listen(PORT, () => console.log(`SSE test server on http://localhost:${PORT}`));
