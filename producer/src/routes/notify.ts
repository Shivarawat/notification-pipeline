import { Router, Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { publishNotification } from '../kafka/producer';
import type { NotificationEvent } from '../types';

const router = Router();

router.post('/', async (req: Request, res: Response) => {
  const { tenantId, userId, message, channel } = req.body;

  if (!tenantId || !userId || !message || !channel) {
    res.status(400).json({ error: 'tenantId, userId, message, and channel are required' });
    return;
  }

  const event: NotificationEvent = {
    messageId: uuidv4(),
    tenantId,
    userId,
    message,
    channel,
    timestamp: new Date().toISOString(),
  };

  await publishNotification(tenantId, JSON.stringify(event));

  console.log(JSON.stringify({ event: 'notification_queued', messageId: event.messageId, tenantId, userId }));

  res.status(202).json({ status: 'accepted', messageId: event.messageId });
});

export default router;
