import { Router, Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { publishNotification } from '../kafka/producer';
import type { NotificationEvent } from '../types';
import { eventsPublished, notifyRequests, publishDuration } from '../metrics';
import { NotifyRequest, validateNotifyRequest } from './validate';

const router = Router();

router.post('/', async (req: Request, res: Response) => {
  const error = validateNotifyRequest(req.body);
  if (error) {
    notifyRequests.inc({ status: '400' });
    res.status(400).json({ error });
    return;
  }
  const { tenantId, userId, message, channel, messageId } = req.body as NotifyRequest;

  const event: NotificationEvent = {
    messageId: messageId ?? uuidv4(),
    tenantId,
    userId,
    message,
    channel,
    timestamp: new Date().toISOString(),
  };

  const endTimer = publishDuration.startTimer();
  try {
    await publishNotification(tenantId, JSON.stringify(event));
  } catch (err) {
    notifyRequests.inc({ status: '503' });
    console.error(JSON.stringify({ event: 'publish_failed', messageId: event.messageId, error: String(err) }));
    res.status(503).json({ error: 'could not queue notification, try again' });
    return;
  } finally {
    endTimer();
  }
  eventsPublished.inc();
  notifyRequests.inc({ status: '202' });

  res.status(202).json({ status: 'accepted', messageId: event.messageId });
});

export default router;
