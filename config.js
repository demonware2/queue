const config = {
  redis: {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD,
  },
  sqlite: {
    filename: process.env.QUEUE_DB_PATH,
  },
  server: {
    port: process.env.SERVER_PORT,
  },
  jobTypes: {
    EMAIL: 'email',
    WHATSAPP: 'whatsapp',
    SMS: 'sms',
    NOTIFICATION: 'notification',
    CRONJOB: 'cronjob',
    RETRY: 'retry',
    DOC_CONVERT: 'doc_convert',
    WEBHOOK: 'webhook',
    DELAYED_INPUT: 'delayed_input',
    TELEGRAM: 'telegram',
    PUSH_NOTIFICATION: 'push_notification',
    AI_SANDBOX: 'ai_sandbox',
    BACKUP: 'backup',
    GIT_DEPLOY: 'git_deploy',
    WEB_CRAWL: 'web_crawl',
    ZOOM_SYNC_DRIVE: 'zoom_sync_drive',
    ZOOM_SCAN_CLOUD: 'zoom_scan_cloud',
    ZOOM_RESCAN_DRIVE: 'zoom_rescan_drive',
    ZOOM_SCAN_TRASH: 'zoom_scan_trash',
    YOUTUBE_UPLOAD: 'youtube_upload',
  },
  workerSettings: {
    defaultCount: 1,
    maxCount: 10,
  },
  whatsapp: {
    limitCount: parseInt(process.env.WHATSAPP_LIMIT_COUNT) || 80,
    limitWindowHours: parseInt(process.env.WHATSAPP_LIMIT_WINDOW_HOURS) || 6,
  },
  youtube: {
    maxDailyUploads: parseInt(process.env.YOUTUBE_DAILY_UPLOAD_LIMIT) || 100,
  }
};

module.exports = config;