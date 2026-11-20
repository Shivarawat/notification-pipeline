export const CHANNELS = ['email', 'sms', 'push'] as const;
const MESSAGE_ID_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;

export interface NotifyRequest {
  tenantId: string;
  userId: string;
  message: string;
  channel: (typeof CHANNELS)[number];
  messageId?: string;
}

// Returns an error message, or null if the body is valid.
export function validateNotifyRequest(body: unknown): string | null {
  const { tenantId, userId, message, channel, messageId } = (body ?? {}) as Record<string, unknown>;
  if (!tenantId || !userId || !message || !channel) {
    return 'tenantId, userId, message, and channel are required';
  }
  if (!CHANNELS.includes(channel as NotifyRequest['channel'])) {
    return `channel must be one of ${CHANNELS.join(', ')}`;
  }
  // Optional client-supplied ID: retrying with the same messageId is delivered at most once.
  if (messageId !== undefined && (typeof messageId !== 'string' || !MESSAGE_ID_PATTERN.test(messageId))) {
    return 'messageId must be 1-128 letters, digits, or . _ : -';
  }
  return null;
}
