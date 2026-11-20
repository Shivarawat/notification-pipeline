import express from 'express';
import notifyRouter from './routes/notify';
import { registry } from './metrics';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', registry.contentType);
  res.send(await registry.metrics());
});

app.use('/notify', notifyRouter);

app.listen(PORT, () => {
  console.log(JSON.stringify({ event: 'producer_started', port: PORT }));
});
