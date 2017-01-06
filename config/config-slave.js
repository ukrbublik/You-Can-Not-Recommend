var config = {};
const deepmerge = require('deepmerge');

config = deepmerge(require('./config-base'), {
	"emf": {
    "clusterServerPort": 7104,
    "factorsWorkingPath": __dirname + '/../data/slave_factors_working',
    "factorsReadyPath": __dirname + '/../data/slave_factors_ready',
    "factorsTempPath": __dirname + '/../data/slave_factors_tmp',
	},
  "api": {
    "apiServerPort": 8011
  }
});

module.exports = config;