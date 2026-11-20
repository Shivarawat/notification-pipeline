import { Router, Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { publishNotification } from '../kafka/producer';
import type { NotificationEvent } from '../types';
import { eventsPublished, notifyRequests, publishDuration } from '../metrics';

const router = Router();

const CHANNELS = ['email', 'sms', 'push'];
const MESSAGE_ID_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;

router.post('/', async (req: Request, res: Response) => {
  const { tenantId, userId, message, channel, messageId } = req.body ?? {};

  if (!tenantId || !userId || !message || !channel) {
    notifyRequests.inc({ status: '400' });
    res.status(400).json({ error: 'tenantId, userId, message, and channel are required' });
    return;
  }
  if (!CHANNELS.includes(channel)) {
    notifyRequests.inc({ status: '400' });
    res.status(400).json({ error: `channel must be one of ${CHANNELS.join(', ')}` });
    return;
  }
  // Optional client-supplied ID: retrying with the same messageId is delivered at most once.
  if (messageId !== undefined && (typeof messageId !== 'string' || !MESSAGE_ID_PATTERN.test(messageId))) {
    notifyRequests.inc({ status: '400' });
    res.status(400).json({ error: 'messageId must be 1-128 letters, digits, or . _ : -' });
    return;
  }

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

  console.log(JSON.stringify({ event: 'notification_queued', messageId: event.messageId, tenantId, userId }));

  res.status(202).json({ status: 'accepted', messageId: event.messageId });
});

export default router;
