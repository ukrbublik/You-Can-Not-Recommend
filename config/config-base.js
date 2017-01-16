var config = {
	"db": {
    "mal": {
      "host": "localhost",
      "port": 5432,
      "database": "malrec",
      "user": "root",
      "password": "toor"
    },
    "ml": {
      "host": "localhost",
      "port": 5432,
      "database": "mlrec",
      "user": "root",
      "password": "toor"
    }
	},
  "redis": {
  },
  "common": {
    "dbType": 'mal', //'ml' (MovieLens), 'mal' (MyAnimeList)
    "maxRating": {
      "mal": 10,
      "ml": 5,
    },
    "minRecommendRating": {
      "mal": 7, //of 10
      "ml": 3.5, //of 5
    },
  },
	"emf": {
    "factorsCount": 100,
    "trainIters": 10,
    "clusterMasterHost": "localhost",
    "clusterMasterPort": 7101,
    "clusterServerPort": 7101
	},
  "api": {
	  "apiServerPort": 8004
  }
};

module.exports = config;
