const config = {
  redis: {
    host: 'localhost',
    port: 6379,
    password: '',
  },
  sqlite: {
    filename: '/var/www/siroum-extension/queue-system/queue_system.db',
  },
  server: {
    port: 3213,
  },
  jobTypes: {
    EMAIL: 'email',
    WHATSAPP: 'whatsapp',
    SMS: 'sms',
    NOTIFICATION: 'notification',
    CRONJOB: 'cronjob',
  },
  workerSettings: {
    defaultCount: 1,
    maxCount: 10,
  },
};

module.exports = config;