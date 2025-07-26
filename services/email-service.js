const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const nodemailer = require('nodemailer');

class EmailService {
  constructor() {
    this.mainTransporter = null;
    this.backupTransporter = null;
    this.mainConfig = null;
    this.backupConfig = null;
    this.serviceConfig = null;
    this.useBackup = false;
    this.configDbPath = process.env.CONFIG_DB_PATH;
    this.logDbPath = process.env.LOG_DB_PATH;
    this.currentModule = 'Global';
  }

  async init(module = 'Global') {
    try {
      this.currentModule = module || 'Global';

      const db = await open({
        filename: this.configDbPath,
        driver: sqlite3.Database,
      });

      this.serviceConfig = await db.get('SELECT * FROM email_configuration WHERE module = ? LIMIT 1', [this.currentModule]);

      if (!this.serviceConfig) {
        console.log(`No service config found for module '${this.currentModule}', falling back to Global config`);
        this.serviceConfig = await db.get('SELECT * FROM email_configuration where id = 1 LIMIT 1');
      }

      if (!this.serviceConfig) {
        throw new Error('Email service configuration not found in database');
      }

      this.mainConfig = await db.get('SELECT * FROM email_setting WHERE module = ? AND type = "main" LIMIT 1', [this.currentModule]);

      if (!this.mainConfig) {
        console.log(`No main email config found for module '${this.currentModule}', falling back to Global config`);
        this.mainConfig = await db.get('SELECT * FROM email_setting WHERE type = "main" LIMIT 1');
      }

      if (!this.mainConfig) {
        throw new Error('Main email configuration not found in database');
      }

      try {
        this.mainTransporter = nodemailer.createTransport({
          host: this.mainConfig.smtp_host,
          port: this.mainConfig.smtp_port,
          secure: this.mainConfig.smtp_crypto === 'ssl',
          auth: {
            user: this.mainConfig.smtp_user,
            pass: this.mainConfig.smtp_pass,
          },
        });

        console.log(`Main email service for module '${this.currentModule}' initialized successfully`);
        this.useBackup = false;
      } catch (mainError) {
        console.error(`Failed to initialize main email service for module '${this.currentModule}':`, mainError);
        this.mainTransporter = null;

        if (this.serviceConfig.is_backup_enabled == 1 && this.serviceConfig.fail_over == 1) {
          console.log(`Attempting to initialize backup email service for module '${this.currentModule}'...`);
          this.useBackup = true;
        } else {
          await db.close();
          throw new Error(`Main email service for module '${this.currentModule}' failed and failover is disabled`);
        }
      }

      if (this.serviceConfig.is_backup_enabled == 1 && this.serviceConfig.fail_over == 1) {
        try {
          this.backupConfig = await db.get('SELECT * FROM email_setting WHERE module = ? AND type = "backup" LIMIT 1', [this.currentModule]);

          if (!this.backupConfig) {
            console.log(`No backup email config found for module '${this.currentModule}', falling back to Global config`);
            this.backupConfig = await db.get('SELECT * FROM email_setting WHERE type = "backup" LIMIT 1');
          }

          if (!this.backupConfig) {
            throw new Error('Backup email configuration not found in database');
          }

          this.backupTransporter = nodemailer.createTransport({
            host: this.backupConfig.smtp_host,
            port: this.backupConfig.smtp_port,
            secure: this.backupConfig.smtp_crypto === 'ssl',
            auth: {
              user: this.backupConfig.smtp_user,
              pass: this.backupConfig.smtp_pass,
            },
          });

          console.log(`Backup email service for module '${this.currentModule}' initialized successfully`);
        } catch (backupError) {
          console.error(`Failed to initialize backup email service for module '${this.currentModule}':`, backupError);
          this.backupTransporter = null;

          if (this.mainTransporter === null) {
            await db.close();
            throw new Error(`Both main and backup email services for module '${this.currentModule}' failed to initialize`);
          }

          this.useBackup = false;
        }
      }

      await db.close();
      return true;
    } catch (error) {
      console.error(`Failed to initialize email service for module '${this.currentModule}':`, error);
      return false;
    }
  }

  async logEmailAttempt(emailOptions, result, error = null) {
    try {
      const db = await open({
        filename: this.logDbPath,
        driver: sqlite3.Database,
      });

      const now = new Date().toISOString();
      const status = error ? 'failed' : 'success';
      const errorMessage = error ? error.message : null;
      const serviceType = this.useBackup ? 'backup' : 'main';
      const messageId = result?.messageId || null;
      const module = emailOptions.module || this.currentModule || 'Global';

      const subject = emailOptions.subject || '';
      const recipients = typeof emailOptions.to === 'string'
        ? emailOptions.to
        : Array.isArray(emailOptions.to)
          ? emailOptions.to.join(', ')
          : '';
      const html = emailOptions.html || '';
      const sender = emailOptions.from || '';
      const cc = typeof emailOptions.cc === 'string'
        ? emailOptions.cc
        : Array.isArray(emailOptions.cc)
          ? emailOptions.cc.join(', ')
          : '';

      await db.run(
        `INSERT INTO email_log_queue (
            timestamp,
            service_type,
            status,
            subject,
            recipients,
            sender,
            html_body,
            message_id,
            error_message,
            cc,
            module
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          now,
          serviceType,
          status,
          subject,
          recipients,
          sender,
          html,
          messageId,
          errorMessage,
          cc,
          module
        ]
      );

      await db.close();
      console.log(`Email attempt logged to database: ${status} (module: ${module})`);
    } catch (logError) {
      console.error('Failed to log email attempt to database:', logError);
    }
  }

  async sendEmail(emailOptions) {
    if (emailOptions.module && emailOptions.module !== this.currentModule) {
      console.log(`Email module changed from '${this.currentModule}' to '${emailOptions.module}', reinitializing...`);
      await this.init(emailOptions.module);
    }

    const transporter = this.useBackup ? this.backupTransporter : this.mainTransporter;
    const config = this.useBackup ? this.backupConfig : this.mainConfig;
    let result = null;

    const moduleForLog = emailOptions.module || this.currentModule || 'Global';

    if (!transporter) {
      const error = new Error(`No email service available for module '${moduleForLog}'`);
      this.logEmailAttempt(emailOptions, null, error);
      throw error;
    }

    try {
      if (!emailOptions.from) {
        emailOptions.from = `"${config.from_name}" <${config.from_email}>`;
      }

      if (!emailOptions.html) {
        if (emailOptions.text) {
          emailOptions.html = emailOptions.text.replace(/\n/g, '<br>');
        } else {
          emailOptions.html = '';
        }
      }

      const { module, text, ...emailToSend } = emailOptions;

      try {
        result = await transporter.sendMail(emailToSend);
        this.logEmailAttempt(emailOptions, result);

        return {
          success: true,
          messageId: result.messageId,
          response: result.response,
          usedBackup: this.useBackup,
          module: moduleForLog
        };
      } catch (sendError) {
        if (!this.useBackup && this.backupTransporter && this.serviceConfig.fail_over) {
          console.log(`Main email service for module '${moduleForLog}' failed, switching to backup`);
          this.useBackup = true;

          this.logEmailAttempt(emailOptions, null, sendError);

          result = await this.backupTransporter.sendMail(emailToSend);
          this.logEmailAttempt(emailOptions, result);

          if (this.serviceConfig.email_notification && this.serviceConfig.admin_email) {
            try {
              const notificationOptions = {
                from: `"${this.backupConfig.from_name}" <${this.backupConfig.from_email}>`,
                to: this.serviceConfig.admin_email,
                subject: `Email Service Failover Activated for ${moduleForLog}`,
                html: `<p>The main email service for module '${moduleForLog}' failed and the system has switched to the backup service.</p><p>Error: ${sendError.message}</p>`
              };

              const notifyResult = await this.backupTransporter.sendMail(notificationOptions);
              this.logEmailAttempt(notificationOptions, notifyResult);
              console.log(`Failover notification sent to admin for module '${moduleForLog}'`);
            } catch (notifyError) {
              console.error(`Failed to send failover notification for module '${moduleForLog}':`, notifyError);
            }
          }

          return {
            success: true,
            messageId: result.messageId,
            response: result.response,
            usedBackup: true,
            module: moduleForLog
          };
        }

        this.logEmailAttempt(emailOptions, null, sendError);
        throw sendError;
      }
    } catch (error) {
      console.error(`Failed to send email for module '${moduleForLog}':`, error);
      if (!result) {
        this.logEmailAttempt(emailOptions, null, error);
      }
      throw error;
    }
  }

  async checkServiceHealth() {
    if (this.useBackup && this.mainTransporter) {
      try {
        await this.mainTransporter.verify();
        console.log(`Main email service for module '${this.currentModule}' has recovered, switching back`);
        this.useBackup = false;

        if (this.serviceConfig.email_notification && this.serviceConfig.admin_email && this.backupTransporter) {
          try {
            const notificationOptions = {
              from: `"${this.backupConfig.from_name}" <${this.backupConfig.from_email}>`,
              to: this.serviceConfig.admin_email,
              subject: `Email Service Recovery for ${this.currentModule}`,
              html: `<p>The main email service for module '${this.currentModule}' has recovered and is now being used again.</p>`
            };

            const result = await this.backupTransporter.sendMail(notificationOptions);
            this.logEmailAttempt(notificationOptions, result);
            console.log(`Recovery notification sent to admin for module '${this.currentModule}'`);
          } catch (notifyError) {
            console.error(`Failed to send recovery notification for module '${this.currentModule}':`, notifyError);
          }
        }

        return true;
      } catch (error) {
        console.log(`Main email service for module '${this.currentModule}' is still unavailable`);
        return false;
      }
    }
    return this.mainTransporter !== null || this.backupTransporter !== null;
  }
}

module.exports = EmailService;