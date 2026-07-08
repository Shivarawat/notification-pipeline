import express from 'express';
import notifyRouter from './routes/notify';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.use('/notify', notifyRouter);

app.listen(PORT, () => {
  console.log(JSON.stringify({ event: 'producer_started', port: PORT }));
});
