/**
 * You Can (Not) Recommend
 * Recommender system
 *
 * @author ukrbublik
 */

const args = process.argv.slice(2);
const express = require('express');
const http = require('http');
const app = express();
const server = http.Server(app);
const nodeCleanup = require('node-cleanup');
const YcnrController = require('./lib/YcnrController');

let configPrefix = process.env.configPrefix ? process.env.configPrefix : 'master';
var config = require('./config/config-'+configPrefix);
if (process.env.dbType)
  config.common.dbType = process.env.dbType;
if (!config.common.maxRating[config.common.dbType])
  throw new Error("Unknown db type " + config.common.dbType 
    + ", please specify it in config (maxRating, minRecommendRating)");

var ctrl = new YcnrController(); //todo: singleton, don't allow multiple processed

let taskType = args[0];
if (taskType == 'import_ml') {
  config.common.dbType = 'ml';
  let dataSize = args[1];
  if (dataSize == undefined)
    dataSize = '100k'; //default
  if (['100k', '1m'].indexOf(dataSize) == -1) {
    console.log("Incorrect param 1: ", dataSize , "Should be one of ", ['100k', '1m']);
  } else {
    do_import_ml(dataSize);
  }
} else if(taskType == 'delete_all_data') {
  do_delete_all_data();
} else if(taskType == 'start') {
  let isMaster = (configPrefix.indexOf('slave') == -1);
  start_server(isMaster);
} else {
  show_cmd_help();
}

function show_cmd_help() {
  console.log("Usage: [configPrefix=..] [dbType=..] " 
    + "node index.js import_ml|delete_all_data|start [other params]");
  console.log("configPrefix: master (default), slave, slave2");
  console.log("dbType: ml for MovieLens, mal for MyAnimeList");
  console.log("import_ml: import from MovieLens data. Param 1: 100k|1m");
  console.log("delete_all_data: clear db, delete calc results. " 
    + "Useful after playing with ML data and switching to production data. " 
    + "Be sure to use it!!!");
  console.log("start: start server");
  console.log("Increase memory of node: --max_old_space_size=<size_MB>");
  process.exit(0);
}

function do_delete_all_data() {
  ctrl.init(true, config).then(() => {
    ctrl.deleteAllData().then(() => {
      ctrl.destroy();
    });
  });
}

function do_import_ml(dataSize) {
  ctrl.init(true, config).then(() => {
    ctrl.importML(dataSize).then(() => {
			ctrl.destroy();
    });
  });
}

function start_server(isMaster) {
  ctrl.init(isMaster, config).then(() => {
    enable_api_emf();
    if (isMaster)
      enable_api_emf_lord();
    server.listen(config.api.apiServerPort, () => {
      console.log('API listening on port ' + config.api.apiServerPort);
    });
  });
}

function enable_api_emf() {
  app.get('/status', (req, res) => {
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.write(ctrl.emf.status);
    res.end();
  });

  function recommendByUserLoginOrId(req, res) {
    let limit = (req.query.limit ? req.query.limit : 20);
    let getUserPromise = req.params.userId ? 
      ctrl.getUserById(req.params.userId) : ctrl.getUserByLogin(req.params.userLogin);
    getUserPromise.then((user) => {
      if (user) {
        return ctrl.recommendItemsForUser(user, limit).then((data) => {
          res.writeHead(200, {'Content-Type': 'application/json'});
          res.write(JSON.stringify(data));
          res.end();
        });
      } else {
        let err = "User not found";
        res.writeHead(404, {'Content-Type': 'application/json'});
        res.write(JSON.stringify(err));
        res.end();
      }
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
    ctrl.emf.gatherClusterNodes().then(() => {
      res.writeHead(200, {'Content-Type': 'text/html'});
      res.write("ok");
      res.end();
    });
  });

  app.get('/train', (req, res) => {
    let err = ctrl.emf.getCanTrainError();
    if (err === null) {
      ctrl.emf.train().then(() => {
        //completed
      }, (err) => {
      	console.error("Train rejection: ", err);
      });
      res.writeHead(200, {'Content-Type': 'text/html'});
      res.write("ok");
      res.end();
    } else {
      res.writeHead(406, {'Content-Type': 'text/html'});
      res.write(err);
      res.end();
    }
  });
}

nodeCleanup(() => {
});

process.on('unhandledRejection', function (err) {
  console.error("!!! Unhandled Rejection", err);
});

process.on('uncaughtException', function (err) {
  console.error("!!! Uncaught Exception", err);
});

