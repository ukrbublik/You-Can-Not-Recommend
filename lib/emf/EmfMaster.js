/**
 * Master process (opposite of worker), can be lord or chief
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
const EmfManager = require('./EmfManager');

//todo: saveCalcResults every iter!

/**
 * 
 */
class EmfMaster extends EmfManager {

  /**
   *
   */
  constructor(isClusterMaster = true) {
    super(false, isClusterMaster);

    this.reqId = 0; //can be use to identify tcp socket requests, see chief's _incrNextPortion()
  }

  /**
   *
   */
  init(config, options = {}, dbConnection = null, redisConnection = null) {
    this.config = config; //tip: to transfer to workers
    return super.init(config, options, dbConnection, redisConnection);
  }


  // -----------------------  workers  -----------------------

  /**
   * 
   */
   createWorkers() {
    if (this.workers)
      return Promise.resolve();
    else return new Promise((resolve, reject) => {
      let isAls = (this.options.alg == 'als');
      this.workers = [];
      let threads = Math.max(
        this.options.numThreadsForRmse, this.options.numThreadsForTrain[this.options.alg] );

      let createdPromises = [];

      for (let i = 0 ; i < threads ; i++) {
        // Create worker
        let child = child_process.fork(__dirname + '/EmfWorkerProcess.js');
        let useForTrain = (i < this.options.numThreadsForTrain[this.options.alg]);
        let useForAlsTrain = useForTrain && isAls;
        let options = {
          process: child,
          id: i,
          state: -1, //-1 - initing, 0 - ready, 1 - busy
          portionBuffer: null, //see prepareWorkerToTrain()
          useForTrain: useForTrain,
        };
        let w = new EmfProcess(options);
        this.workers[i] = w;

        // Bind events
        w.on('completedPortion', (data) => {
          this.wm_completedPortion(w, data);
        });
        w.on('error', (err) => {
          console.error('[Worker#'+w.id+']', err);
        });
        w.once('exit', (code, signal) => {
          console.log('[Worker#'+w.id+']' + ' Exited with code ' + code);
          this.destroy();
        });
        w.emit('create', {
          isClusterMaster: this.isLord,
          workerId: w.id, 
          options: this.options, 
          config: this.config,
        });
        createdPromises.push(new Promise((resolve, reject) => {
          w.once('created', () => {
            resolve();
          });
        }));
      }

      Promise.all(createdPromises).then(() => {
        resolve();
      });
    });
   }


  /**
   * 
   */
   initWorkers() {  
    let preparedPromises = [];

    for (let i = 0 ; i < this.workers.length ; i++) {
      let w = this.workers[i];
      if (w.state == 1) {
        return Promise.reject({error: "Worker #" + w.id + " is busy"});
      }
      w.state = -1;
    }

    this.detachWorkPortionBuffers();
    this.createWorkPortionBuffers();

    // Send init event
    for (let w of this.workers) {
      w.emit('prepareToTrain', {
        stats: this.stats,
        options: this.options,
      });
      preparedPromises.push(new Promise((resolve, reject) => {
        w.once('preparedToTrain', () => {
          w.state = 0;
          resolve();
        });
      }));
    }

    return Promise.all(preparedPromises);
  }

  /**
   *
   */
  prepareWorkersToTrain() {
    return this.createWorkers().then(() => {
      return this.initWorkers();
    });
  }

  /**
   *
   */
  broadcastMessageToWorkers(msg, data = {}) {
    for (let w of this.workers) {
      w.emit(msg, data);
    }
  }

  /**
   *
   */
  createWorkPortionBuffers() {
    //todo: combine buffers for als (pb.als*) and rmse (pb.als*), if using same portion size

    let isAls = (this.options.alg == 'als');
    let isSgd = (this.options.alg == 'sgd');
    let ratingsInPortionAls = !isAls ? -1 : 
      Math.max(this.maxRatingsInPortion.byUser, this.maxRatingsInPortion.byItem);
    let maxRowsInPortionAls = !isAls ? -1 : 
      Math.max(this.maxRowsInPortion.byUser, this.maxRowsInPortion.byItem);
    let ratingsInPortionRmse = Math.max(this.maxRatingsInPortion.rmseValidate, 
      this.maxRatingsInPortion.rmseTest);
    let maxRowsInPortionRmse = Math.max(this.maxRowsInPortion.rmseValidate, 
      this.maxRowsInPortion.rmseTest);

    this.options.shared.portionBufferShmKeys = {};
    for (let i = 0 ; i < this.workers.length ; i++) {
      let w = this.workers[i];
      let useForTrain = w.useForTrain;
      let useForAlsTrain = useForTrain && isAls;

      w.portionBuffer = {};
      let pb = {
        status: 0, // 0 - free, 1 - fetching, 2 - ready, 3 - working
        portionNo: -1,
        factorsBuffer: useForAlsTrain && this.options.lowmem ? 
          shm.create(maxRowsInPortionAls * this.factorsCount, this.TypedArrayKey) : null,
      };
      if (isSgd) {
        pb.singVals = shm.create(this.options.ratingsInPortionForSgd, this.TypedArrayKey);
        pb.singIndx = shm.create(this.options.ratingsInPortionForSgd * 2 + 1, 'Int32Array');
      } else if(useForAlsTrain) {
        pb.alsVals = shm.create(ratingsInPortionAls, this.TypedArrayKey);
        pb.alsIndx = shm.create(ratingsInPortionAls, 'Int32Array');
        pb.alsRows = shm.create(maxRowsInPortionAls * 2 + 1, 'Int32Array');
      }
      pb.rmseVals = shm.create(ratingsInPortionRmse, this.TypedArrayKey);
      pb.rmseIndx = shm.create(ratingsInPortionRmse, 'Int32Array');
      pb.rmseRows = shm.create(maxRowsInPortionRmse * 2 + 1, 'Int32Array');
      w.portionBuffer.forWork = pb;

      if (this.options.usePortionsCache) {
        let pb = {
          status: 0, // 0 - free, 1 - fetching, 2 - ready
          portionNo: -1,
          factorsBuffer: null,
        };
        if (isSgd) {
          pb.singVals = new this.TypedArrayClass(this.options.ratingsInPortionForSgd);
          pb.singIndx = new Int32Array(this.options.ratingsInPortionForSgd * 2 + 1);
        } else if(useForAlsTrain) {
          pb.alsVals = new this.TypedArrayClass(ratingsInPortionAls);
          pb.alsIndx = new Int32Array(ratingsInPortionAls);
          pb.alsRows = new Int32Array(maxRowsInPortionAls * 2 + 1);
        }
        pb.rmseVals = new this.TypedArrayClass(ratingsInPortionRmse);
        pb.rmseIndx = new Int32Array(ratingsInPortionRmse);
        pb.rmseRows = new Int32Array(maxRowsInPortionRmse * 2 + 1);
        w.portionBuffer.forCache = pb;
      }

      pb = {
        factorsBuffer: useForAlsTrain && this.options.lowmem ? 
          w.portionBuffer.forWork.factorsBuffer.key : null,
      };
      if (isSgd) {
        pb.singVals = w.portionBuffer.forWork.singVals.key;
        pb.singIndx = w.portionBuffer.forWork.singIndx.key;
      } else if(useForAlsTrain) {
        pb.alsVals = w.portionBuffer.forWork.alsVals.key;
        pb.alsIndx = w.portionBuffer.forWork.alsIndx.key;
        pb.alsRows = w.portionBuffer.forWork.alsRows.key;
      }
      pb.rmseVals = w.portionBuffer.forWork.rmseVals.key;
      pb.rmseIndx = w.portionBuffer.forWork.rmseIndx.key;
      pb.rmseRows = w.portionBuffer.forWork.rmseRows.key;
      this.options.shared.portionBufferShmKeys[w.id] = pb;
    }

  }

  /**
   *
   */
  detachWorkPortionBuffers() {
    if (this.workers) {
      for (let i = 0 ; i < this.workers.length ; i++) {
        let w = this.workers[i];
        if (w.portionBuffer) {
          let shmKeys = this.options.shared.portionBufferShmKeys[w.id];
          for (let k in shmKeys) {
            if (shmKeys[k] !== null)
              shm.detach(shmKeys[k]);
          }
          w.portionBuffer = null;
          this.options.shared.portionBufferShmKeys[w.id] = null;
        }
      }
    }
  }

  // -----------------------  cluster  -----------------------

  /**
   *
   */
  broadcastMessageToCluster(msg, data, callback) {
    if (!this.options.useClustering)
      return;

    for (let nodeId in this.clusterNodes) {
      let client = this.clusterNodes[nodeId];
      client.sendPacket(msg, data, callback);
    }
  }

  /**
   *
   */
  broadcastMessageAndStreamToCluster(msg, data, stream, callback) {
    if (!this.options.useClustering)
      return;

    for (let nodeId in this.clusterNodes) {
      let client = this.clusterNodes[nodeId];
      client.sendPacketAndStreams(msg, data, [stream], callback);
    }
  }

  /**
   *
   */
  whenGotMessageFromCluster(msg) {
    if (!this.options.useClustering)
      return Promise.resolve();
    else return new Promise((resolve, reject) => {
      let doneNodeIds = [];
      let totalCnt = Object.keys(this.myClients).length;
      let checkDone = () => {
        if (doneNodeIds.length == totalCnt) {
          resolve();
        }
      };
      for (let nodeId in this.myClients) {
        let client = this.myClients[nodeId];
        client.once(msg, () => {
          doneNodeIds.push(nodeId);
          checkDone();
        });
        client.once("close", () => {
          //client disconnected
          if (doneNodeIds.indexOf(nodeId) == -1)
            doneNodeIds.push(nodeId);
          checkDone();
        });
      }
      checkDone();
    });
  }

  /**
   *
   * Tip: need to call initCluster() to reconnect to cluster
   */
  disconnectFromCluster() {
    for (let nodeId in this.clusterNodes) {
      let client = this.clusterNodes[nodeId];
      client.disconnect();
    }
    this.clusterNodes = {};

    for (let clientId in this.myClients) {
      let client = this.myClients[clientId];
      client.disconnect();
    }
    this.myClients = {};

    if (this.isChief) {
      this.lordClient.disconnect();
      this.lordClient = null;
      this.chiefServer.close();
    } else { //this.isLord
      this.lordServer.close();
    }
  }

  // -----------------------    -----------------------

  /**
   * Prepare factors storage
   * Callaed in prepareToTrain()
   */
  prepareSharedFactors() {
    return this._loadSharedFactorsForTrain().then(([recreated, extended]) => {
      if (this.isLord) {
        if (recreated) {
          this.initSharedFactorsRandom(0, 0);
        } else if(extended) {
          this.initSharedFactorsRandom(this.lastCalcInfo.totalUsersCount, 
            this.lastCalcInfo.totalItemsCount);
        }
      }
    });
  }

  /**
   * Don't call manually, see EmfChief.cl_alsTrainStepChief(), EmfLord.alsTrainStepLord()
   * @param string stepType 'byUser', 'byItem'
   */
  _startAlsTrainStep(stepType) {
    this.workType = 'train';
    this.usedThreads = this.options.numThreadsForTrain[this.options.alg];
    this.stepType = stepType;
    let totalPortions = this.portionsCount[this.stepType];
    this.workProgress = new ProgressBar('Iter #' + this.trainIter + ', step ' + this.stepType 
        + ' [:bar] :current/:total :elapsed/:etas', { total: totalPortions });

    this.completedPortions = 0;
    this.requestingPortions = 0;

    for (let t = 0 ; t < this.usedThreads ; t++) {
      let w = this.workers[t];
      w.emit('startTrainStep', {
        stepType: this.stepType 
      });
    }

    this.m_prepareNextPortions();
  }

  /**
   * Don't call manually, see EmfChief.cl_startCalcRmse(), EmfLord.calcRmse()
   * @param string stepType 'rmseValidate', 'rmseTest'
   */
  _startCalcRmse(stepType, useGlobalAvgShift) {
    useGlobalAvgShift = useGlobalAvgShift && this.options.alg == 'als';
    this.calcGlobalAvgShift = !useGlobalAvgShift;
    this.workType = 'rmse';
    this.usedThreads = this.options.numThreadsForRmse;
    this.stepType = stepType;
    let totalPortions = this.portionsCount[this.stepType];
    this.workProgress = new ProgressBar('Calcing ' + stepType
      + ' [:bar] :current/:total :elapsed/:etas', { total: totalPortions });

    this.completedPortions = 0;
    this.requestingPortions = 0;

    this.globalAvgShift = this.calcGlobalAvgShift ? 0 : this.globalAvgShift;
    for (let t = 0 ; t < this.usedThreads ; t++) {
      let w = this.workers[t];
      w.emit('startCalcRmse', {
        stepType: stepType, 
        globalAvgShift: this.globalAvgShift
      });
    }

    this.m_prepareNextPortions();
  }

  /**
   * 
   */
  _endCalcRmse(globalAvgShift) {
    this.globalAvgShift = globalAvgShift;
    this.workProgress.terminate();
  }

  /**
   * 
   */
  /*abstract*/ _incrNextPortion(wantPortions, callback) {
    throw new Exception("abstract");
  }

  // -----------------------  m <-> w  -----------------------

  /**
   * 
   */
  m_prepareNextPortions() {
    //method to fetch portion data from db
    let fetchMethod;
    if (this.workType == 'train') {
      if (this.options.alg == 'als')
        fetchMethod = 'm_fetchPortionTrainAlsOrRmse';
      else if (this.options.alg == 'sgd')
        fetchMethod = 'm_fetchPortionTrainSgd';
    } else
      fetchMethod = 'm_fetchPortionTrainAlsOrRmse';

    //how many portions want to take?
    let totalPortions = this.portionsCount[this.stepType];
    let keys = this.options.usePortionsCache ? ['forWork', 'forCache'] : ['forWork'];
    let maxRequestingPortions = this.usedThreads * keys.length;
    let freeBuffers = 0;
    for (let k of keys) {
      for (let t = 0 ; t < this.usedThreads ; t++) {
        let w = this.workers[t];
        let buf = w.portionBuffer[k];
        if (buf.status == 0) {
          freeBuffers++;
        }
      }
    }
    let wantPortions = Math.min(freeBuffers, maxRequestingPortions - this.requestingPortions);

    //todo_later: лучше распределять порции между нодами в кластере? 
    //пример: rmse. всего 10 порций, 2 ноды по 4 ядра. сейчас первая заберет 8 (4+4 с кэшем), 
    // вторая 2.

    if (wantPortions > 0) {
      this.requestingPortions += wantPortions;
      this._incrNextPortion(wantPortions, (portions) => {
        this.requestingPortions -= wantPortions;
        if (portions.length > 0) {
          let p = 0;
          let portionNo = portions[p];
          loop:
          for (let k of keys) {
            for (let t = 0 ; t < this.usedThreads ; t++) {
              let w = this.workers[t];
              let buf = w.portionBuffer[k];
              if (buf.status == 0) {
                buf.status = 1;
                buf.portionNo = portionNo;
                portionNo++;
                p++;

                this[fetchMethod](w, k);

                if (p == portions.length)
                  break loop;
              }
            }
          }
          assert(p == portions.length);
        }
      });
    }
  }

  //todo: m_fetch*() - handle possible db errors

  /**
   * 
   */
  m_fetchPortionTrainAlsOrRmse(w, k) {
    let filterDatasetType = (this.stepType == 'rmseValidate' ? "= 2" : 
      (this.stepType == 'rmseTest' ? "= 3" : "IN (1, 2)"));

    let buf = w.portionBuffer[k];
    let rowIdFrom = buf.portionNo == 0 ? 0 : 
      this.portionsRowIdTo[this.stepType][buf.portionNo - 1];
    let rowIdTo = this.portionsRowIdTo[this.stepType][buf.portionNo];
    let sql;
    if (this.stepType == 'byItem') {
      sql = "\
        SELECT r.user_list_id as c, r.item_id as r, r.rating \
        FROM malrec_ratings as r " + 
        (this.options.trainAllUsersItems ? "\
        INNER JOIN malrec_items as i \
         ON i.id = r.item_id AND i.is_used_for_train = true" : "") + "\
        WHERE r.item_id > ${rowIdFrom} AND r.item_id <= ${rowIdTo} \
         AND r.dataset_type " + filterDatasetType + " \
        ORDER BY r.item_id \
      ";
    } else {
      sql = "\
        SELECT r.user_list_id as r, r.item_id as c, r.rating \
        FROM malrec_ratings as r " + 
        (this.options.trainAllUsersItems ? "\
        INNER JOIN malrec_users as u \
         ON u.list_id = r.user_list_id AND u.is_used_for_train = true" : "") + "\
        WHERE r.user_list_id > ${rowIdFrom} AND r.user_list_id <= ${rowIdTo} \
         AND r.dataset_type " + filterDatasetType + " \
        ORDER BY r.user_list_id, r.item_id \
      ";
    }
    let t1 = Date.now();
    //console.log("Fetching portion #" + portionNo + ' ' + k + '...');
    this.db.any(sql, {
      rowIdFrom: rowIdFrom,
      rowIdTo: rowIdTo,
    }).then((data) => {
      let t2 = Date.now();
      //console.log("Fetched portion #" + portionNo + ' ' + k + " (" +  (t2-t1) + "ms)");
      this.m_processFetchedPortionAlsOrRmse(w, k, data);
    });
  }

  /**
   * 
   */
  m_fetchPortionTrainSgd(w, k) {
    let buf = w.portionBuffer[k];
    let limit = this.options.ratingsInPortionForSgd;
    let offset = buf.portionNo * limit;
    let sql = "\
      SELECT user_list_id, item_id, rating \
      FROM malrec_ratings \
      WHERE dataset_type IN (1, 2) \
      ORDER BY rand, user_list_id, item_id \
      LIMIT $(limit) \
      OFFSET $(offset) \
    ";
    let t1 = Date.now();
    this.db.any(sql, {
      limit: limit,
      offset: offset,
    }).then((data) => {
      let t2 = Date.now();
      this.m_processFetchedPortionSgd(w, k, data);
    });
  }

  /**
   * 
   */
  m_processFetchedPortionAlsOrRmse(w, k, data) {
    let buf = w.portionBuffer[k];

    let bufRows, bufIndx, bufVals;
    if (this.stepType == 'rmseValidate' || this.stepType == 'rmseTest') {
      [bufRows, bufIndx, bufVals] = [buf.rmseRows, buf.rmseIndx, buf.rmseVals];
    } else {
      [bufRows, bufIndx, bufVals] = [buf.alsRows, buf.alsIndx, buf.alsVals];
    }

    let t1 = Date.now();
    let last_r, r = 0, cols = 0;
    for (let i = 0 ; i < data.length ; i++) {
      //1-based in db, 0-based in matrix
      data[i].c--;
      data[i].r--;

      assert(i < bufVals.length); //if fails, modify splitToPortions()
      bufVals[i] = data[i].rating;
      bufIndx[i] = data[i].c;

      if (i == 0)
        last_r = data[i].r;
      if (last_r != data[i].r || i == (data.length - 1)) {
        //Save stats and start calcing stats for new row
        assert((1 + r*2 + 1) < bufRows.length); //if fails, modify splitToPortions()
        bufRows[1 + r*2] = last_r;
        bufRows[1 + r*2 + 1] = cols;
        last_r = data[i].r;
        cols = 0;
        r++;
      }
      cols++;
    }

    let t2 = Date.now();
    //console.log("Converted portion #" + buf.portionNo + ' ' + k 
    // + " (" +  (t2-t1) + "ms) - " + r + ' rows, ' + i + ' ratings');
    bufRows[0] = r;
    buf.status = 2;

    if(this.m_handleReadyPortions())
      this.m_prepareNextPortions();
  }

  /**
   * 
   */
  m_processFetchedPortionSgd(w, k, data) {
    let buf = w.portionBuffer[k];

    let t1 = Date.now();
    for (let i = 0 ; i < data.length ; i++) {
      //1-based in db, 0-based in matrix
      data[i].user_list_id--;
      data[i].item_id--;

      buf.singVals[i] = data[i].rating;
      buf.singIndx[1 + i*2] = data[i].user_list_id;
      buf.singIndx[1 + i*2 + 1] = data[i].item_id;
    }
    let t2 = Date.now();
    //console.log("Converted portion #" + buf.portionNo + ' ' + k 
    // + " (" +  (t2-t1) + "ms) - " + data.length + ' ratings');
    buf.singIndx[0] = data.length;
    buf.status = 2;

    if(this.m_handleReadyPortions())
      this.m_prepareNextPortions();
  }

  /**
   * @return bool need to prepare next portions?
   */
  m_handleReadyPortions() {
    let freeCached = 0;
    for (let t = 0 ; t < this.usedThreads ; t++) {
      let w = this.workers[t];
      let bufWork = w.portionBuffer.forWork;

      if (this.options.usePortionsCache) {
        let bufCache = w.portionBuffer.forCache;
        // forCache -> forWork (copy)
        if (bufWork.status == 0 && bufCache.status == 2) {
          if (this.workType == 'train' && this.options.alg == 'als') {
            bufWork.alsVals.set( bufCache.alsVals );
            bufWork.alsRows.set( bufCache.alsRows );
            bufWork.alsIndx.set( bufCache.alsIndx );
          } else if(this.workType == 'train' && this.options.alg == 'sgd') {
            bufWork.singVals.set( bufCache.singVals );
            bufWork.singIndx.set( bufCache.singIndx );
          } else if(this.workType == 'rmse') {
            bufWork.rmseVals.set( bufCache.rmseVals );
            bufWork.rmseRows.set( bufCache.rmseRows );
            bufWork.rmseIndx.set( bufCache.rmseIndx );
          }
          bufWork.portionNo = bufCache.portionNo;
          bufWork.status = 2;
          bufCache.status = 0;
          freeCached++;
          bufCache.portionNo = -1;
        }
      }

      // send ready work buffers to workers
      if (bufWork.status == 2) {
        bufWork.status = 3;
        //console.log('Worker #' + w.id + ' started to work on portion #' + bufWork.portionNo);
        w.state = 1;
        w.emit((this.workType == 'train' ? 
            (this.options.alg == 'als' ? 'calcTrainAlsPortion' : 'calcTrainSgdPortion') : 
            'calcRmsePortion'), {
          portionNo: bufWork.portionNo 
        });
      }
    }

    return (freeCached > 0);
  }


  /**
   * msg.portionNo
   * msg.time
   * msg.rowsRange (for als)
   * msg.ratingsInPortion (for als)
   * msg.memoryUsage
   * msg.rSumDiff2, msg.rCnt (for this.workType == 'rmse')
   */
  wm_completedPortion(w, msg) {
    /*
    //todo_later: see EmfLord.splitToPortions(), split better?
    if (this.workType == 'train' && this.options.alg == 'als')
      console.log(' Worker#' + w.id + ' portion #' + msg.portionNo 
        + ' (' + msg.rowsRange.cnt + ' rows, ' + msg.ratingsInPortion + ' ratings, ' 
        + msg.time + ' ms)');
    */
    w.state = 0;

    let writeFileStream = null;
    if (this.workType == 'train' && this.options.alg == 'als' && 
        (this.options.lowmem || this.options.useClustering)) {
      let readStream = this.getReadStreamForFactorsBuffer(
        this.stepType, msg.rowsRange, w.portionBuffer.forWork.factorsBuffer);
      if (this.options.lowmem) {
        // write to file
        writeFileStream = this.getWriteStreamToSaveCalcedFactors(this.stepType, msg.rowsRange);
        readStream.pipe(writeFileStream);
      }

      // send to cluster
      this.broadcastMessageAndStreamToCluster("alsSaveCalcedFactors", msg, readStream);
      //callback - m_completedPortion()
    }

    if (this.isChief && this.workType == 'rmse') {
      // send to Lord
      this.lordClient.sendPacket("rmseSaveCalcs", {
        rowsRange: msg.rowsRange,
        portionNo: msg.portionNo,
        rSumDiff2: msg.rSumDiff2,
        rCnt: msg.rCnt,
        rSum: msg.rSum,
      });
      //callback - m_completedPortion()
    }

    w.portionBuffer.forWork.status = 0;
    w.portionBuffer.forWork.portionNo = -1;

    this.m_handleReadyPortions();
    this.m_prepareNextPortions();

    if (writeFileStream)
      writeFileStream.once('finish', () => {
        this.m_completedPortion(msg);
      });
    else
      this.m_completedPortion(msg);
  }

  /**
   * If nodeId != null, it's callback of event 'alsSaveCalcedFactors' or 'rmseSaveCalcs', 
   *  triggered from some cluster node.
   * Else called from wm_completedPortion() after calcing on current node
   */
  m_completedPortion(msg, nodeId = null) {
    //if (nodeId)
    //  console.log('Node#' + nodeId + ' completed portion #' + msg.portionNo 
    // + ' (' + msg.time + ' ms)');
    if (this.isLord && nodeId !== null) {
      let ind = this.busyPortions[nodeId].indexOf(msg.portionNo);
      if (ind != -1)
        this.busyPortions[nodeId].splice(ind, 1);
    }
    this.completedPortions++;
    this.workProgress.tick();
    let totalPortions = this.portionsCount[this.stepType];

    if (this.isLord && this.workType == 'rmse') {
      this.rSumDiff2 += msg.rSumDiff2;
      this.rCnt += msg.rCnt;
      this.rSum += msg.rSum;
    }

    if (this.completedPortions == totalPortions) {
      if (this.isLord && this.workType == 'rmse') {
        this.rmse = Math.sqrt( 1.0 * this.rSumDiff2 / this.rCnt );
        this.predAvg = msg.rSum / msg.rCnt;
        if (this.calcGlobalAvgShift) {
          this.globalAvgShift = this.stats.totalRatingsAvg - this.predAvg;
        }
      }
      this.eventEmitter.emit('stepComplete');
    }
  }

  /**
   * 
   */
  whenAllWorkersAreDead() {
    if (this.workers) {
      let self = this;
      function areAllWorkersDead() {
        let areDead = true;
        for (let w of self.workers) {
          if(w.process && w.process.connected)
            areDead = false;
        }
        return areDead;
      }

      return new Promise((resolve, reject) => {
        if(areAllWorkersDead()) {
          resolve();
        } else {
          for (let w of this.workers) {
            if (w.process)
              w.process.on('exit', (code, signal) => {
                if(areAllWorkersDead()) {
                    resolve();
                }
              });
          }
        }
      });
    } else {
      return Promise.resolve();
    }
  }

  /**
   * 
   */
  killWorkers() {
    if (this.workers) {
      for (let w of this.workers) {
        w.emit('kill');
      }
    }
  }

}
var cls = EmfMaster; //for using "cls.A" as like "self::A" inside class

module.exports = EmfMaster;
