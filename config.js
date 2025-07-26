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
  },
  workerSettings: {
    defaultCount: 1,
    maxCount: 10,
  },
};

module.exports = config;