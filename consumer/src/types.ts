export interface NotificationEvent {
  messageId: string;
  tenantId: string;
  userId: string;
  message: string;
  channel: 'email' | 'sms' | 'push';
  timestamp: string;
}
