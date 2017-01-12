var config = {
	"db": {
    "host": "localhost",
    "port": 5432,
    "database": "malrec",
    "user": "root",
    "password": "toor"
	},
  "redis": {
  },
  "common": {
    "dbType": 'ml', //'ml' (MovieLens), 'mal' (MyAnimeList)
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
    "clusterMasterHost": "localhost",
    "clusterMasterPort": 7101,
    "clusterServerPort": 7101
	},
  "api": {
	  "apiServerPort": 8004
  }
};

module.exports = config;
