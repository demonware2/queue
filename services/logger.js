const levels = { error: 0, warn: 1, info: 2, debug: 3 };

const envLevel = (process.env.LOG_LEVEL || 'info').toLowerCase();
const current = levels[envLevel] !== undefined ? levels[envLevel] : levels.info;

function format(msg, args) {
  if (!args || args.length === 0) return msg;
  return [msg, ...args].join(' ');
}

module.exports = {
  debug: (...args) => {
    if (current >= levels.debug) console.log(...args);
  },
  info: (...args) => {
    if (current >= levels.info) console.log(...args);
  },
  warn: (...args) => {
    if (current >= levels.warn) console.warn(...args);
  },
  error: (...args) => {
    console.error(...args);
  }
};

