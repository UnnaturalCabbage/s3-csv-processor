module.exports = {
  server: {
    port: process.env.PORT || 3001,
  },
  aws: {},
  mongo: {
    url:
      process.env.MONGO_URL ||
      "",
    database: "service",
  },
  redis: {
    url: "",
  },
};
