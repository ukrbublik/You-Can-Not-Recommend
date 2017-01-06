/**
 * You Can (Not) Recommend
 * Train, recommend
 *
 * @author ukrbublik
 */

const fs = require('fs');
const EmfFactory = require('./lib/emf/Emf').EmfFactory;
const MalScanner = require('./lib/mal/MalScanner');
var emf = null;
var mal = null;
const args = process.argv.slice(2);
const ProgressBar = require('progress');
const express = require('express');
const http = require('http');
const app = express();
const server = http.Server(app);
const nodeCleanup = require('node-cleanup');

var configPrefix = args[0] ? args[0] : 'base';
const config = require('./config/config-'+configPrefix);


if (args[1] == 'import_ml') {
  var dataSize = args[2];
  if (dataSize == undefined)
    dataSize = '100k'; //default
  if (['100k', '1m'].indexOf(dataSize) == -1) {
    console.log("Incorrect param 1: ", dataSize , "Should be one of ", ['100k', '1m']);
  } else {
    import_ml(dataSize);
  }
} else if(args[1] == 'delete') {
  do_delete();
} else if(args[1] == 'master') {
  start_master();
} else if(args[1] == 'slave') {
  start_slave();
} else {
  show_cmd_help();
}

function do_delete() {
  emf = EmfFactory.createLord();

  emf.on("destroyed", () => {
  	process.exit(0);
  });

  emf.init(config).then(() => {
    emf.deleteAllData().then(() => {
      emf.destroy();
    });
  });
}

function import_ml(dataSize) {
  emf = EmfFactory.createLord();

  emf.on("destroyed", () => {
  	process.exit(0);
  });

  emf.init(config).then(() => {
    emf.importML(dataSize).then(() => {
			emf.destroy();
    });
  });
}

function start_master() {
  emf = EmfFactory.createLord();

  emf.on("destroyed", () => {
  	process.exit(0);
  });

  Promise.all([
    emf.init(config)
  ]).then(() => {
    enable_api_emf();
    enable_api_emf_lord();

    server.listen(config.api.apiServerPort, () => {
      console.log('API listening on port ' + config.api.apiServerPort);
    });
  });
}

function start_slave() {
  emf = EmfFactory.createChief();

  emf.on("destroyed", () => {
  	process.exit(0);
  });

  Promise.all([
    emf.init(config)
  ]).then(() => {
    enable_api_emf();

    server.listen(config.api.apiServerPort, () => {
      console.log('API listening on port ' + config.api.apiServerPort);
    });
  });
}

function show_cmd_help() {
  console.log("Usage: node config-prefix index.js import_ml|clear|master|slave [other params]");
  console.log("import_ml: import from MovieLens data. Param 1: 100k|1m");
  console.log("clear: clear db, delete calc results. Useful after playing with ML data and switching to production data (mal)");
  console.log("Increase memory of node: --max_old_space_size=<size_MB>");
  process.exit(0);
}

function enable_api_emf() {
  app.get('/status', (req, res) => {
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.write(emf.status);
    res.end();
  });

  function recommendByUserLoginOrId(req, res) {
    let userListId = null,
      limit = (req.query.limit ? req.query.limit : 20);
    let getUserPromise = req.params.userId ? 
      emf.getUserById(req.params.userId) : emf.getUserByLogin(req.params.userLogin);
    getUserPromise.then((data) => {
      userListId = data.list_id;
      emf.recommendItemsForUser(userListId, limit).then((data) => {
        res.writeHead(200, {'Content-Type': 'application/json'});
        res.write(JSON.stringify(data));
        res.end();
      }, (err) => {
        res.writeHead(404, {'Content-Type': 'application/json'});
        res.write(JSON.stringify(err));
        res.end();
      });
    });
  }


  app.get('/user/:userLogin/recommend', (req, res) => {
    recommendByUserLoginOrId(req, res);
  });

  app.get('/userid/:userId/recommend', (req, res) => {
    recommendByUserLoginOrId(req, res);
  });
}

function enable_api_emf_lord() {
  app.get('/gather', (req, res) => {
    emf.gatherClusterNodes().then(() => {
      res.writeHead(200, {'Content-Type': 'text/html'});
      res.write("ok");
      res.end();
    });
  });

  app.get('/train', (req, res) => {
    let err = emf.getCanTrainError();
    if (err === null) {
      emf.train().then(() => {
        //completed
      }, (err) => {
      	console.log("Train rejection: ", err);
      });
      res.writeHead(200, {'Content-Type': 'text/html'});
      res.write("ok");
      res.end();
    } else {
      res.writeHead(405, {'Content-Type': 'text/html'});
      res.write(err);
      res.end();
    }
  });
}

setInterval(() => {
  if (emf) {
    emf.fixPeakMemoryUsage();
  }
}, 1000);

nodeCleanup(() => {

});

process.on('unhandledRejection', function (err) {
  console.error("!!! Unhandled Rejection", err);
});

process.on('uncaughtException', function (err) {
  console.error("!!! Uncaught Exception", err);
});

