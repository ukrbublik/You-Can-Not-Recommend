var config = {};
const deepmerge = require('deepmerge');

config = deepmerge(require('./config-base'), {
	"emf": {
    "clusterServerPort": 7105,
    "factorsWorkingPath": __dirname + '/../data/slave2_factors_working',
    "factorsReadyPath": __dirname + '/../data/slave2_factors_ready',
    "factorsTempPath": __dirname + '/../data/slave2_factors_tmp',
	},
  "api": {
    "apiServerPort": 8007
  }
});

module.exports = config;