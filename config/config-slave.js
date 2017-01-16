var config = {};
const deepmerge = require('deepmerge');

config = deepmerge(require('./config-base'), {
	"emf": {
    "clusterServerPort": 7104,
	},
  "api": {
    "apiServerPort": 8011
  }
});

module.exports = config;