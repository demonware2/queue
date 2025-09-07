const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const nodemailer = require('nodemailer');

class EmailService {
  constructor() {
    // Per-module state map
    // module => { mainTransporter, backupTransporter, mainConfig, backupConfig, serviceConfig, useBackup }
    this.modules = new Map();
    this.configDbPath = process.env.CONFIG_DB_PATH;
    this.logDbPath = process.env.LOG_DB_PATH;
  }

  async init(module = 'Global') {
    try {
      const db = await open({
        filename: this.configDbPath,
        driver: sqlite3.Database,
      });
      const targetModule = module || 'Global';

      let serviceConfig = await db.get('SELECT * FROM email_configuration WHERE module = ? LIMIT 1', [targetModule]);

      if (!serviceConfig) {
        console.log(`No service config found for module '${targetModule}', falling back to Global config`);
        serviceConfig = await db.get('SELECT * FROM email_configuration where id = 1 LIMIT 1');
      }

      if (!serviceConfig) {
        throw new Error('Email service configuration not found in database');
      }

      let mainConfig = await db.get('SELECT * FROM email_setting WHERE module = ? AND type = "main" LIMIT 1', [targetModule]);

      if (!mainConfig) {
        console.log(`No main email config found for module '${targetModule}', falling back to Global config`);
        mainConfig = await db.get('SELECT * FROM email_setting WHERE type = "main" LIMIT 1');
      }

      if (!mainConfig) {
        throw new Error('Main email configuration not found in database');
      }

      let mainTransporter = null;
      let backupTransporter = null;
      let backupConfig = null;
      let useBackup = false;

      // Try to build main transporter
      try {
        mainTransporter = nodemailer.createTransport({
          host: mainConfig.smtp_host,
          port: mainConfig.smtp_port,
          secure: mainConfig.smtp_crypto === 'ssl',
          auth: {
            user: mainConfig.smtp_user,
            pass: mainConfig.smtp_pass,
          },
        });
        console.log(`Main email service for module '${targetModule}' initialized successfully`);
      } catch (mainError) {
        console.error(`Failed to initialize main email service for module '${targetModule}':`, mainError);
        mainTransporter = null;
      }

      // Initialize backup transporter if module has backup enabled (regardless of fail_over setting)
      if (serviceConfig.is_backup_enabled == 1) {
        try {
          backupConfig = await db.get('SELECT * FROM email_setting WHERE module = ? AND type = "backup" LIMIT 1', [targetModule]);
          if (!backupConfig) {
            console.log(`No backup email config found for module '${targetModule}', falling back to Global config`);
            backupConfig = await db.get('SELECT * FROM email_setting WHERE type = "backup" LIMIT 1');
          }

          if (backupConfig) {
            try {
              backupTransporter = nodemailer.createTransport({
                host: backupConfig.smtp_host,
                port: backupConfig.smtp_port,
                secure: backupConfig.smtp_crypto === 'ssl',
                auth: {
                  user: backupConfig.smtp_user,
                  pass: backupConfig.smtp_pass,
                },
              });
              console.log(`Backup email service for module '${targetModule}' initialized successfully`);
            } catch (backupCreateErr) {
              console.error(`Failed to create backup transporter for module '${targetModule}':`, backupCreateErr);
              backupTransporter = null;
            }
          } else {
            console.log(`Backup email configuration not found in database for module '${targetModule}'`);
          }
        } catch (backupError) {
          console.error(`Failed to initialize backup email service for module '${targetModule}':`, backupError);
          backupTransporter = null;
        }
      }

      // Decide initial useBackup only if allowed and needed
      if (!mainTransporter && backupTransporter && serviceConfig.fail_over == 1) {
        useBackup = true;
      }

      this.modules.set(targetModule, {
        mainTransporter,
        backupTransporter,
        mainConfig,
        backupConfig,
        serviceConfig,
        useBackup,
      });

      await db.close();
      if (!mainTransporter && !backupTransporter) {
        console.warn(`No available email transporter for module '${targetModule}' (main and backup unavailable)`);
        return false;
      }
      return true;
    } catch (error) {
      console.error(`Failed to initialize email service for module '${module || 'Global'}':`, error);
      return false;
    }
  }

  async logEmailAttempt(emailOptions, result, error = null, moduleOverride = null, usedBackup = false) {
    let db;
    try {
      db = await open({
        filename: this.logDbPath,
        driver: sqlite3.Database,
      });

      const now = new Date().toISOString();
      const status = error ? 'failed' : 'success';
      const errorMessage = error ? error.message : null;
      const serviceType = usedBackup ? 'backup' : 'main';
      const messageId = result?.messageId || null;
      const module = moduleOverride || emailOptions.module || 'Global';

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
    } finally {
      if (db) {
        try { await db.close(); } catch (_) {}
      }
    }
  }

  async sendEmail(emailOptions) {
    const targetModule = emailOptions.module || 'Global';

    if (!this.modules.has(targetModule)) {
      console.log(`Email module '${targetModule}' not initialized yet, initializing...`);
      await this.init(targetModule);
    }

    if (!this.modules.has(targetModule)) {
      const error = new Error(`Failed to initialize email service for module '${targetModule}'`);
      await this.logEmailAttempt(emailOptions, null, error, targetModule, false);
      throw error;
    }

    const state = this.modules.get(targetModule);
    const transporter = state.useBackup ? state.backupTransporter : state.mainTransporter;
    const config = state.useBackup ? state.backupConfig : state.mainConfig;
    let result = null;

    const moduleForLog = targetModule;

    if (!transporter) {
      const error = new Error(`No email service available for module '${moduleForLog}'`);
      this.logEmailAttempt(emailOptions, null, error, moduleForLog, false);
      throw error;
    }

    try {
      if (!emailOptions.from) {
        if (!config || !config.from_email || !config.from_name) {
          const cfgErr = new Error(`Sender identity misconfigured for module '${moduleForLog}': missing from_name/from_email`);
          this.logEmailAttempt(emailOptions, null, cfgErr, moduleForLog, state.useBackup);
          throw cfgErr;
        }
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
        this.logEmailAttempt(emailOptions, result, null, moduleForLog, state.useBackup);

        return {
          success: true,
          messageId: result.messageId,
          response: result.response,
          usedBackup: state.useBackup,
          module: moduleForLog
        };
      } catch (sendError) {
        if (!state.useBackup && state.backupTransporter && state.serviceConfig.fail_over) {
          console.log(`Main email service for module '${moduleForLog}' failed, switching to backup`);
          state.useBackup = true;

          this.logEmailAttempt(emailOptions, null, sendError, moduleForLog, false);

          result = await state.backupTransporter.sendMail(emailToSend);
          this.logEmailAttempt(emailOptions, result, null, moduleForLog, true);

          if (state.serviceConfig.email_notification && state.serviceConfig.admin_email) {
            try {
              const notificationOptions = {
                from: `"${state.backupConfig.from_name}" <${state.backupConfig.from_email}>`,
                to: state.serviceConfig.admin_email,
                subject: `Email Service Failover Activated for ${moduleForLog}`,
                html: `<p>The main email service for module '${moduleForLog}' failed and the system has switched to the backup service.</p><p>Error: ${sendError.message}</p>`
              };

              const notifyResult = await state.backupTransporter.sendMail(notificationOptions);
              this.logEmailAttempt(notificationOptions, notifyResult, null, moduleForLog, true);
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

        this.logEmailAttempt(emailOptions, null, sendError, moduleForLog, state.useBackup);
        throw sendError;
      }
    } catch (error) {
      console.error(`Failed to send email for module '${moduleForLog}':`, error);
      if (!result) {
        this.logEmailAttempt(emailOptions, null, error, moduleForLog, false);
      }
      throw error;
    }
  }

  async checkServiceHealth() {
    let anyRecovered = false;
    for (const [mod, state] of this.modules.entries()) {
      if (state.useBackup && state.mainTransporter) {
        try {
          await state.mainTransporter.verify();
          console.log(`Main email service for module '${mod}' has recovered, switching back`);
          state.useBackup = false;

          if (state.serviceConfig.email_notification && state.serviceConfig.admin_email && state.backupTransporter) {
            try {
              const notificationOptions = {
                from: `"${state.backupConfig.from_name}" <${state.backupConfig.from_email}>`,
                to: state.serviceConfig.admin_email,
                subject: `Email Service Recovery for ${mod}`,
                html: `<p>The main email service for module '${mod}' has recovered and is now being used again.</p>`
              };

              const result = await state.backupTransporter.sendMail(notificationOptions);
              this.logEmailAttempt(notificationOptions, result, null, mod, true);
              console.log(`Recovery notification sent to admin for module '${mod}'`);
            } catch (notifyError) {
              console.error(`Failed to send recovery notification for module '${mod}':`, notifyError);
            }
          }

          anyRecovered = true;
        } catch (error) {
          console.log(`Main email service for module '${mod}' is still unavailable`);
        }
      }
    }
    return anyRecovered || this.hasAnyBackupActive() === false;
  }

  hasAnyBackupActive() {
    for (const state of this.modules.values()) {
      if (state.useBackup) return true;
    }
    return false;
  }
}

module.exports = EmailService;
