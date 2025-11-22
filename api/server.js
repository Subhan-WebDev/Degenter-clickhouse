// api/server.js
import http from 'http';
import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
import { info } from '../lib/log.js';
import tokensRouter from './routes/tokens.js';
import tradesRouter from './routes/trades.js';
import swapRouter from './routes/swap.js';
import watchlistRouter from './routes/watchlist.js';
import alertsRouter from './routes/alerts.js';
import { startWS } from './ws.js';

const app = express();
app.use(cors());
app.use(express.json());
app.use(morgan('dev'));

app.get('/health', (req, res) => res.json({ ok: true }));

app.use('/tokens', tokensRouter);
app.use('/trades', tradesRouter);
app.use('/swap', swapRouter);
app.use('/watchlist', watchlistRouter);
app.use('/alerts', alertsRouter);

// IMPORTANT: create a raw HTTP server and attach WS to it
const PORT = parseInt(process.env.API_PORT || '8003', 10);
const server = http.createServer(app);

// boot WebSocket server on path /ws
startWS(server, { path: '/ws' });

server.listen(PORT, () => info(`api + ws listening on :${PORT}`));
