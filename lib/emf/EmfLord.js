/**
 * Master process, main node in cluster
 *
 * @author ukrbublik
 */

const {
  TcpSocket, ReadBufferStream, WriteBufferStream, shm, numCPUs, child_process, cpp_utils, 
  vectorious, Matrix, Vector, SpMatrix, SpVector, BLAS,
  net, os, http, fs, assert,
  deepmerge, _, co, pgp, redis, EventEmitter, nodeCleanup, ProgressBar,
  Helpers, EmfBase, EmfProcess
} = require('./EmfBase');
const EmfMaster = require('./EmfMaster');
const extractZip = require('extract-zip');



/**
 * 
 */
class EmfLord extends EmfMaster {

  /**
   *
   */
  constructor() {
    super(true);

    this.busyPortions = {};
    this.droppedPortions = [];
  }

  /**
   * @param string size '1m' or '100k'
   */
  importML(size) {
    let dataPath = __dirname + '/../../data';
    let mlPath = dataPath + '/' + 'ml-' + size;
    let url = (size == '1m' ? "http://files.grouplens.org/datasets/movielens/ml-1m.zip" : 
      "http://files.grouplens.org/datasets/movielens/ml-100k.zip");
    let zipFile = dataPath + '/' + (size == '1m' ? "ml-1m.zip" : "ml-100k.zip");

    this._status = "importing";
    return fs.existsSync(mlPath) || fs.existsSync(zipFile) ? Promise.resolve() : 
      new Promise((resolve, reject) => {
        console.log("Downloading " + url + ' ...');
        let zipFileStream = fs.createWriteStream(zipFile);
        let request = http.get(url, (response) => {
          if (response.statusCode == 200) {
            response.pipe(zipFileStream);
            zipFileStream.once('finish', () => {
              resolve();
            });
          } else reject("HTTP status is " + response.statusCode);
        });
    }).then(() => {
      return fs.existsSync(mlPath) ? Promise.resolve() : 
        new Promise((resolve, reject) => {
          console.log("Extracting " + zipFile + ' ...');
          extractZip(zipFile, {
            dir: dataPath,
          }, (err) => {
            if (!err) {
              if (fs.existsSync(mlPath)) {
                let files = fs.readdirSync(mlPath);
                for (let file of files) {
                  fs.chmodSync(mlPath + '/' + file, parseInt('0644', 8));
                }
                if (size == '1m') {
                  console.log('Replacing delimeter \'::\' to \'~\' ...');
                  for (let file of ['users.dat', 'movies.dat', 'ratings.dat']) {
                    let data = fs.readFileSync(mlPath + '/' + file, 'utf8');
                    let result = data.replace(/::/g, '~');
                    fs.writeFileSync(mlPath + '/' + file, result);
                  }
                }
                resolve();
              } else reject("Folder does not exist after unzipping: " + mlPath);
            } else reject(err);
          });
        });
    }).then(() => {
      console.log("Deleting all data ...");
      return this._deleteAllData();
    }).then(() => {
      console.log('Importing from ML ' + size + ' ...');
      let func = 'malrec_import_ml_' + size;
      return this.db.func(func, [ mlPath ]);
    }).then(() => {
      this._status = "ready";
      console.log('Done.');
      return;
    });
  }

  /**
   *
   */
  _deleteAllData() {
    this.detachSharedFactors();
    this.detachWorkPortionBuffers();
    this.deleteCalcResultsSync();
    return this.db.func("malrec_delete_all_data", []);
  }

  /**
   *
   */
  deleteAllData() {
    this._status = "importing";
    console.log("Deleting all data ...");
    return this._deleteAllData().then(() => {
      this._status = "ready";
      console.log('Done.');
      return;    
    });
  }

  /**
   * @param bool forceResplitToSets - update data sets random distribution on train/validate/test
   */
  splitDataForTrain(forceResplitToSets = false) {
    return this.splitToSets(forceResplitToSets)
      .then(() => this.getStats())
      .then(() => this.splitToPortions());
  }

  /**
   *
   */
  getStats() {
    return Promise.all([
      this.db.one("\
        select max(list_id) as max_id \
        from malrec_users \
      "),
      this.db.one("\
        select max(id) as max_id \
        from malrec_items \
      "),
      this.db.one("\
        select count(list_id) as cnt \
        from malrec_users \
        where is_used_for_train = true \
      "),
      this.db.one("\
        select count(id) as cnt \
        from malrec_items \
        where is_used_for_train = true \
      "),
      this.db.any("\
        select list_id as id, ratings_count as cnt, avg_rating as avg \
        from malrec_users \
        where is_used_for_train = true \
        order by list_id \
      "),
      this.db.any("\
        select id, ratings_count as cnt, avg_rating as avg \
        from malrec_items \
        where is_used_for_train = true \
        order by id \
      "),
    ]).then(data => {
      this.totalUsersCount = parseInt(data[0].max_id);
      this.totalItemsCount = parseInt(data[1].max_id);
      this.trainUsersCount = parseInt(data[2].cnt);
      this.trainItemsCount = parseInt(data[3].cnt);
      let ratingsPerUser = data[4];
      let ratingsPerItem = data[5];

      this.stats.maxRatingsPerUser = 0;
      this.stats.maxRatingsPerItem = 0;
      this.stats.ratingsCntPerUser = [];
      this.stats.ratingsAvgPerUser = [];
      this.stats.ratingsCntPerItem = [];
      this.stats.ratingsAvgPerItem = [];
      this.stats.trainUsersRatingsCount = 0;
      for (let row of ratingsPerUser) {
        row.id = parseInt(row.id);
        row.cnt = parseInt(row.cnt);
        row.avg = parseFloat(row.avg);
        if (row.cnt > this.stats.maxRatingsPerUser)
          this.stats.maxRatingsPerUser = row.cnt;
        this.stats.ratingsCntPerUser[row.id - 1] = row.cnt;
        this.stats.ratingsAvgPerUser[row.id - 1] = row.avg;
        this.stats.trainUsersRatingsCount += row.cnt;
      }
      this.stats.trainItemsRatingsCount = 0;
      for (let row of ratingsPerItem) {
        row.id = parseInt(row.id);
        row.cnt = parseInt(row.cnt);
        row.avg = parseFloat(row.avg);
        if (row.cnt > this.stats.maxRatingsPerItem)
          this.stats.maxRatingsPerItem = row.cnt;
        this.stats.ratingsCntPerItem[row.id - 1] = row.cnt;
        this.stats.ratingsAvgPerItem[row.id - 1] = row.avg;
        this.stats.trainItemsRatingsCount += row.cnt;
      }

      console.log(
        "Users:", this.trainUsersCount,
        "\nItems:", this.trainItemsCount,
        "\nRatings:", [this.stats.trainUsersRatingsCount, this.stats.trainItemsRatingsCount]
      );
      return;
    });
  }

  /**
   * 
   */
  splitToSets(forceResplit = false) {
    let doSplit, splitAllRatings;
    if (forceResplit || this.lastCalcInfo 
      && !_.isEqual(this.options.dataSetDistr, this.lastCalcInfo.dataSetDistr)) {
      //distribution % changed, need to resplit
      splitAllRatings = true;
      doSplit = true;
    } else if (!this.isFirstTrain()) {
      //new ratings added, need to add them to split
      doSplit = true;
      splitAllRatings = false;
    } else {
      //for first train or first train with new options
      doSplit = false;
      splitAllRatings = true;
    }

    let pcts = this.options.dataSetDistr;
    if (doSplit)
      console.log("Splitting" + (splitAllRatings ? " all" : " new") 
        + " ratings to sets train/validate/test ", pcts);
    let func = splitAllRatings ? 'malrec_resplit_to_sets' : 'malrec_split_more_to_sets';
    let t1 = Date.now();
    let promise = !doSplit ? Promise.resolve() 
      : this.db.func(func, [ pcts[0], pcts[1], pcts[3] ]);
    return promise.then((data) => {
      return Promise.all([
        this.db.one("\
          select avg(r.rating) as avg \
          from malrec_ratings as r \
          where dataset_type is not null \
        "),
        this.db.any("\
          select dataset_type, count(rating) as cnt \
          from malrec_ratings \
          where dataset_type is not null \
          group by dataset_type \
        "),
      ]);
    }).then(rows => {
      this.stats.totalRatingsAvg = rows[0].avg;

      let totalSplitCnt = {
        'train': 0,
        'validate': 0,
        'test': 0,
      };
      for (let row of rows[1]) {
        totalSplitCnt[row.dataset_type] = parseInt(row.cnt);
      }

      let total = totalSplitCnt.train + totalSplitCnt.validate + totalSplitCnt.test;
      let newPcts = [ 
        totalSplitCnt.train / total * 100, 
        totalSplitCnt.validate / total * 100, 
        totalSplitCnt.test / total * 100 ];
      let isOkSplit;
      if (doSplit && splitAllRatings) {
        isOkSplit = true;
      } else {
        isOkSplit = (total > 0);
        for (let i = 0 ; i < 3 ; i++) {
          if ( Math.abs(newPcts[i] - pcts[i]) > 1 )
            isOkSplit = false;
        }
      }

      if (!isOkSplit) {
        return this.splitToSets(true);
      } else {
        this.stats.ratingsCountTrain = totalSplitCnt.train + totalSplitCnt.validate;
        this.stats.ratingsCountValidate = totalSplitCnt.validate;
        this.stats.ratingsCountTest = totalSplitCnt.test;

        let t2 = Date.now();
        console.log("Data are splitted to sets train/validate/test %", newPcts, 
          (!doSplit ? "" : "- complete in " + (t2-t1) + " ms"),
          "\nTrain:", this.stats.ratingsCountTrain,
          "\nValidate:", this.stats.ratingsCountValidate,
          "\nTest:", this.stats.ratingsCountTest
        );

        console.log("Updating stats...");
        return this.db.func("malrec_upd_stats", []).then(() => {
          console.log("Stats updated.");
        });
      }
    });
  }

  /**
   * 
   */
  splitToPortions() {
    if (this.stats.trainUsersRatingsCount == 0 || this.stats.trainItemsRatingsCount == 0)
      return Promise.resolve();

    return new Promise((resolve, reject) => {
      if (this.options.alg == 'als') {
        this.portionsRowIdTo = {};
        // Calc optimal distribution of portions of data to calc in parallel

        // if 100.000 ratings and 1.000 users (rows), then avg 100 ratings per row
        // if ratingsInPortion = 5.000, then will be 20 portions, 50 rows in portion
        //todo_later: also consider that portion with 1 item and 1000 ratings will be calced
        // faster than portion with 100 items and 10 ratings per each
        for (let i = 0 ; i < 2 ; i++) {
          let stepType = (i == 0 ? "byUser" : "byItem");
          let rowsCnt = (stepType == "byUser" ? this.trainUsersCount : this.trainItemsCount);
          let ratingsCntPer = (stepType == "byUser" ? this.stats.ratingsCntPerUser 
            : this.stats.ratingsCntPerItem);
          let maxRatingsPerRow = (stepType == "byUser" ? this.stats.maxRatingsPerUser 
            : this.stats.maxRatingsPerItem);
          let ratingsCount = (stepType == "byUser" ? this.stats.trainUsersRatingsCount 
            : this.stats.trainItemsRatingsCount);
          let ratingsInPortion = this.options.ratingsInPortionForAls[stepType];
          let avgPortionsCount = Math.ceil(ratingsCount / ratingsInPortion);
          let avgRowsInPortion = Math.floor(rowsCnt / avgPortionsCount);
          if (avgPortionsCount < this.options.numThreadsForTrain[this.options.alg]) {
            avgPortionsCount = this.options.numThreadsForTrain[this.options.alg];
            ratingsInPortion = Math.ceil(ratingsCount / avgPortionsCount);
            avgRowsInPortion = Math.floor(rowsCnt / avgPortionsCount);
          }
          if (avgRowsInPortion < 1) {
            avgRowsInPortion = 1;
            avgPortionsCount = rowsCnt;
            ratingsInPortion = Math.ceil(ratingsCount / avgPortionsCount);
          }
          if (ratingsInPortion < maxRatingsPerRow) {
            ratingsInPortion = maxRatingsPerRow;
            avgPortionsCount = Math.ceil(ratingsCount / ratingsInPortion);
            avgRowsInPortion = Math.floor(rowsCnt / avgPortionsCount);
          }

          let portionsRowIdTo = [];
          let p = 0, rtgs = 0, rows = 0, maxRows = 0;
          for (let id in ratingsCntPer) {
            let cnt = ratingsCntPer[id];
            id = parseInt(id);
            if ((rtgs + cnt) > ratingsInPortion) {
              rtgs = 0;
              rows = 0;
              p++;
            }
            rtgs += cnt;
            rows++;
            if (rows > maxRows)
              maxRows = rows;
            portionsRowIdTo[p] = id + 1; //id is 0-based, for db we need 1-based
          }

          this.portionsRowIdTo[stepType] = portionsRowIdTo;
          this.portionsCount[stepType] = portionsRowIdTo.length;
          this.maxRatingsInPortion[stepType] = ratingsInPortion;
          this.maxRowsInPortion[stepType] = maxRows;
        }

        console.log(
          "Work portions:", this.portionsCount,
          "\nMax ratings in portion:", this.maxRatingsInPortion, 
          "\nMax rows in portion:", this.maxRowsInPortion
        );
      } else if (this.options.alg == 'sgd') {
        this.portionsCount['sgd'] = Math.ceil(this.stats.ratingsCountTrain / 
          this.options.ratingsInPortionForRmse);
      }

      this.portionsCount['rmseValidate'] = Math.ceil(this.stats.ratingsCountValidate / 
        this.options.ratingsInPortionForRmse);
      this.portionsCount['rmseTest'] = Math.ceil(this.stats.ratingsCountTest / 
        this.options.ratingsInPortionForRmse);

      resolve();
    });
  }

  /**
   * @param bool forceResplitToSets false to use previous data sets distribution
   */
  prepareToTrain(forceResplitToSets = false) {
    console.log('Preparing to train...');
    // Once prepare db for train
    // Create `rand` column in `malrec_ratings` table for SGD, else remove it
    let func = this.options.alg == 'sgd' ? 'malrec_add_ratings_rand' 
      : 'malrec_drop_ratings_rand';
    return this.db.func(func, []).then(() => {
      // Fix date
      return this.db.one("select now() as now")
    }).then((data) => {
      this.calcDate = data.now;
      let trainAllRatings = (this.options.alg == 'sgd' || !this.lastCalcInfo || this.lastCalcInfo 
        && !_.isEqual(this.options.dataSetDistr, this.lastCalcInfo.dataSetDistr));
      return this.db.func('malrec_fix_for_train', [trainAllRatings]);
    }).then(() => {
      return this.splitDataForTrain(forceResplitToSets);
    }).then(() => {
      if (this.trainUsersCount == 0 && this.trainItemsCount == 0)
        return Promise.reject({"code": "no_data", "error": "No data to train"});
      return this.prepareSharedFactors();
    }).then(() => {
      return this.prepareWorkersToTrain();
    }).then(() => {
      console.log("Total shared memory used: " + Helpers.humanFileSize(shm.getTotalSize()));
      console.log('Prepared to train');
      return;
    });
  }
  
  /**
   *
   */
  initCluster() {
    if (this.options.useClustering)
      return this.initClusterLord();
    else
      return Promise.resolve();
  }

  /**
   *
   */
  initClusterLord() {
    this.clusterNodes = {}; //see this.gatherClusterNodes()
    this.myClients = {};
    var lastNodeId = 1;

    this.lordServer = net.createServer({}, (clientSocket) => {
      let client = new TcpSocket(true, clientSocket);
      let nodeId = 's' + (lastNodeId++);

      client.on("register", (data) => {
        if (this.status != "ready") {
          client.destroy("Sorry, server state is " + this.status + ", try again later");
        } else {
          client.data.nodeId = nodeId;
          client.data.host = data.host;
          client.data.port = data.port;
          this.myClients[nodeId] = client;
          console.log("Cluster node #" + nodeId + " registered: ", data);
          client.sendPacket("registered", {
            nodeId: nodeId
          });
        }
      })

      client.on('error', (err) => {
        console.log("Cluster node #" + nodeId + " client socker error: ", err);
      });

      client.on('close', () => {
        if (this.myClients[nodeId]) {
          console.log("Cluster node #" + nodeId + " disconnected." + (this.busyPortions[nodeId] ?
           " He left unfinished portions: " + this.busyPortions[nodeId] : ""));
          delete this.myClients[nodeId];
          if (this.busyPortions[nodeId]) {
            this.droppedPortions.push(...this.busyPortions[nodeId]);
            delete this.busyPortions[nodeId];
          }
          this.m_prepareNextPortions();
        }
      });

      client.on('getStats', () => {
        client.sendPacket('setStats', {
          stats: this.stats,
          options: _.pick(this.options, 
            'als', 'useDoublePrecision', 'factorsCount', 'trainIters')
        });
      });

      client.on('getNextPortions', (data) => {
        this.m_incrNextPortion(data.wantPortions, (portions) => {
          this.busyPortions[nodeId].push(...portions);
          client.sendPacket('retNextPortions_'+data.reqId, {
            portions: portions
          });
        });
      });

      client.on("alsSaveCalcedFactors", (data) => {
        let stream = this.getWriteStreamToSaveCalcedFactors(this.stepType, data.rowsRange);
        client.receiveStreams([stream], () => {
          this.m_completedPortion(data, client.data.nodeId);
        });
      });

      client.on('rmseSaveCalcs', (data) => {
        this.m_completedPortion(data);
      });

      client.on('getFactors', (data) => {
        client.sendPacketAndStreams('setFactors', null, this.getFactorsReadStreams());
      });
    });
    this.lordServer.listen(this.options.clusterServerPort, () => {
      console.log("Listening cluster on port " + this.options.clusterServerPort);
    });

    return Promise.resolve();
  }

  /**
   * 
   */
  gatherClusterNodes() {
    if (!this.options.useClustering)
      return Promise.resolve();
    else return new Promise((resolve, reject) => {
      if (Object.keys(this.myClients).length)
        console.log('Let cluster slaves to connect with each other (full-mesh)...');

      let meetCnt = Object.keys(this.myClients).length;
      let metCnt = 0;
      let connectCnt = Object.keys(this.myClients).length;
      let connectedCnt = 0;
      let checkTotalMetCnt = () => {
        if (metCnt == meetCnt && connectedCnt == connectCnt) {
          if (connectedCnt)
            console.log('Cluster is gathered: ' + connectedCnt + ' slaves');
          resolve();
        }
      }

      for (let clientId in this.myClients) {
        let client = this.myClients[clientId];
        let meetNodes = {};
        for (let _clientId in this.myClients) {
          if (_clientId !== clientId) {
            let _client = this.myClients[_clientId];
            meetNodes[_clientId] = {
              host: _client.data.host,
              port: _client.data.port
            };
          }
        }
        client.sendPacket("meetCluster", {
          nodes: meetNodes
        }, () => {
          client.once("metCluster", () => {
            metCnt++;
            checkTotalMetCnt();
          });
        });
      }

      for (let nodeId in this.myClients) {
        let _client = this.myClients[nodeId];
        if (this.clusterNodes[nodeId]) {
          //already connected
          connectedCnt++;
        } else {
          let nodeClient = null;
          let nodeClientSocket = net.connect(_client.data.port, _client.data.host, () => {
            nodeClient = new TcpSocket(false, nodeClientSocket);

            nodeClient.sendPacket("setMyNodeId", {
              "nodeId": 'm'
            }, () => {
              nodeClient.once("gotYourNodeId", () => {
                console.log("Connected to cluster node #" + nodeId);
                this.clusterNodes[nodeId] = nodeClient;
                connectedCnt++;
                checkTotalMetCnt();
              });
            });

            nodeClient.on('close', () => {
              console.log("Disconnected from cluster node #" + nodeId);
              delete this.clusterNodes[nodeId];
            });

            nodeClient.on('error', (err) => {
              console.log("Cluster node #" + nodeId + " server socker error: ", err);
            });
          });
        }
      }

      checkTotalMetCnt();
    });
  }

  /**
   *
   */
  prepareClusterToTrain() {
    if (!this.options.useClustering || Object.keys(this.clusterNodes).length == 0)
      return Promise.resolve();
    else {
      console.log("Preparing cluster nodes to train...");
      this.broadcastMessageToCluster("prepareToTrain", {
        lastCalcDate: this.lastCalcDate
      });
      return this.whenGotMessageFromCluster("preparedToTrain").then(() => {
        console.log('Cluster nodes are ready to train with me.');
        return;
      });
    }
  }

  /**
   * 
   */
  getCanTrainError() {
    let err = null;
    if(this.status == "training") {
      err = "Training is already started";
    } else if(this.status != "ready") {
      err = "Not ready to train. Status is " + this.status;
    }
    return err;
  }

  /**
   * 
   */
  train() {
    let err = this.getCanTrainError();
    if (err !== null) {
      return Promise.reject(err);
    } else {
      this._status = "preparing";
      return this.prepareToTrain().catch(err => {
        if (err.code == "no_data") {
          console.log("No data to train");
          this._status = "ready";
          //tip: malrec_fix_for_train was called at prepareToTrain()
          return this.db.func('malrec_unfix_for_train', [])
          .then(() => Promise.reject(err));
        } else throw err;
      }).then(() => {
        this._status = "gathering";
        return this.gatherClusterNodes();
      }).then(() => {
        this._status = "syncing";
        return this.prepareClusterToTrain();
      }).then(() => {
        console.log('*** Starting train...');
        this._status = "training";
        this.broadcastMessageToCluster("startTrain");
        this.trainIter = 0;
        return Helpers.promiseWhile(() => (this.trainIter < this.options.trainIters), () => {
          //console.log('>>> Training iteration #' + this.trainIter + ' ...');
          return this.initTrainIter()
          .then(() => (this.options.alg == 'als' ? this.alsTrainIter() : this.sgdTrainIter()))
          .then(() => this.calcRmse('rmseValidate', false))
          .then(() => this.calcRmse('rmseTest', false))
          .then(() => this.calcRmse('rmseTest', true))
          .then(() => {
            this.trainIter++;
          });
        });
      }).then(() => {
        this.calcCnt++;
        console.log('Saving calc results...');
        this.broadcastMessageToWorkers("endTrain");
        this.broadcastMessageToCluster("endTrain", {
          info: this.getCalcInfo(),
        });
        return Promise.all([
          this.whenGotMessageFromCluster("endedTrain"),
          this.saveCalcResults(this.getCalcInfo())
        ]).then(() => {
          this._status = "ready";
          console.log('Peak memory usage: ' + this.formatPeakMemoryUsage());
          console.log('*** Training complete');
          //tip: malrec_fix_for_train was called at prepareToTrain()
          return this.db.func('malrec_unfix_for_train', []);
        });
      });
    }
  }

  /**
   *
   */
  initTrainIter() {
    if (this.options.alg == 'sgd') {
      //is there better option?
      //randomizing indexes in memory can be quicker, but requires a LOT of memory
      console.log('Randomizing ratings order...');
      let t1 = Date.now();
      return this.db.query("\
        update malrec_ratings \
        set rand = random() * 4294967294 - 2147483647 \
        where dataset_type in ('train', 'validate') \
      ").then(() => {
        let t2 = Date.now();
        console.log('Randomizing ratings order done in ' + (t2-t1) + 'ms');
        return;
      });
    } else {
      return Promise.resolve();
    }
  }

  /**
   *
   */
  alsTrainIter() {
    //2 steps - first fix item vectrors and calc user vectors, then vice versa
    return this.alsTrainStep('byUser')
      .then(() => this.alsTrainStep('byItem'));
  }

  /**
   * @param string stepType 'byUser', 'byItem'
   */
  alsTrainStep(stepType) {
    this.nextPortion = 0;
    this.busyPortions = {};
    for (let nodeId in this.clusterNodes) {
      this.busyPortions[nodeId] = [];
    }
    this.droppedPortions = [];

    this._startAlsTrainStep(stepType);

    this.broadcastMessageToCluster("startAlsTrainStep", {
      trainIter: this.trainIter,
      stepType: stepType,
    });

    return new Promise((resolve, reject) => {
      this.eventEmitter.once('stepComplete' , () => {
        this.broadcastMessageToCluster("alsTrainStepEnd");
        resolve();
      });
    });
  }

  /**
   * 
   */
  _incrNextPortion(wantPortions, callback) {
    this.m_incrNextPortion(wantPortions, callback);
  }

  /**
   * 
   */
  m_incrNextPortion(wantPortions, callback) {
    let portions = [];
    let totalPortions = this.portionsCount[this.stepType];
    while (portions.length < wantPortions && this.droppedPortions.length) {
      portions.push(this.droppedPortions.shift());
    }
    while (this.nextPortion < totalPortions && portions.length < wantPortions) {
      portions.push(this.nextPortion++);
    }
    callback(portions);
  }

  /**
   *
   */
  sgdTrainIter() {    
    this.workType = 'train';
    this.usedThreads = this.options.numThreadsForTrain[this.options.alg];
    this.stepType = 'sgd';
    this.nextPortion = 0;
    this.completedPortions = 0;
    this.requestingPortions = 0;
    let totalPortions = this.portionsCount[this.stepType];
    this.workProgress = new ProgressBar('Iter #' + this.trainIter 
        + ' [:bar] :current/:total :elapsed/:etas', { total: totalPortions });

    for (let t = 0 ; t < this.usedThreads ; t++) {
      let w = this.workers[t];
      w.emit('startTrainStep', {
        stepType: this.stepType 
      });
    }

    this.m_prepareNextPortions();

    return new Promise((resolve, reject) => {
      this.eventEmitter.once('stepComplete' , () => {
        resolve();
      });
    });
  }



  /**
   * @param string stepType 'rmseValidate', 'rmseTest'
   */
  calcRmse(stepType, useGlobalAvgShift) {
    if (useGlobalAvgShift && this.options.alg != 'als')
       return Promise.resolve();
    if (this.options.dataSetDistr[1] == 0 && stepType == 'rmseValidate')
       return Promise.resolve();
    if (this.options.dataSetDistr[2] == 0 && stepType == 'rmseTest')
       return Promise.resolve();

    this.nextPortion = 0;
    this.busyPortions = {};
    for (let nodeId in this.clusterNodes) {
      this.busyPortions[nodeId] = [];
    }
    this.droppedPortions = [];

    this.rSum = 0;
    this.rSumDiff2 = 0;
    this.rCnt = 0;

    this._startCalcRmse(stepType, useGlobalAvgShift);

    this.broadcastMessageToCluster("startCalcRmse", {
      stepType: stepType,
      useGlobalAvgShift: useGlobalAvgShift,
    });

    return new Promise((resolve, reject) => {
      this.eventEmitter.once('stepComplete' , () => {
        this._endCalcRmse(this.globalAvgShift);
        this.broadcastMessageToCluster("endCalcRmse", {
          globalAvgShift: this.globalAvgShift,
        });
        console.log(stepType + ' = ' + this.rmse 
          + (useGlobalAvgShift ? ' (applied shift ' + this.globalAvgShift + ')' : '')
          + (this.calcGlobalAvgShift ? ' (shift = ' + this.globalAvgShift + ')' : ''));
        resolve();
      });
    });
  }

}
var cls = EmfLord; //for using "cls.A" as like "self::A" inside class

module.exports = EmfLord;
