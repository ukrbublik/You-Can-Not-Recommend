var config = {};
const deepmerge = require('deepmerge');

config = deepmerge(require('./config-base'), {
  "emf": {
    "clusterServerPort": 7101,
  },
  "api": {
    "apiServerPort": 8004
  }
});

module.exports = config;
