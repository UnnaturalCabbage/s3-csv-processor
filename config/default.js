const { deferConfig: defer } = require(`config/defer`);

module.exports = {
  server: {
    port: process.env.PORT || 3001,
  },
  aws: {},
  mongo: {
    url: process.env.MONGO_URL || "",
    database: "service",
  },
  redis: {
    host: "localhost",
    port: 6379,
    url: defer(function () {
      return `redis://${this.redis.host}:${this.redis.port}`;
    }),
  },
};
