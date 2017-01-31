/**
 * Base
 *
 * @author ukrbublik
 */

const TcpSocket = require('quick-tcp-socket').TcpSocket;
const ReadBufferStream = require('quick-tcp-socket').ReadBufferStream;
const WriteBufferStream = require('quick-tcp-socket').WriteBufferStream;
const vectorious = require('vectorious-plus'),
  Matrix = vectorious.Matrix,
  Vector = vectorious.Vector,
  SpMatrix = vectorious.SpMatrix,
  SpVector = vectorious.SpVector,
  BLAS = vectorious.BLAS;
const EmfProcess = require('./EmfProcess');
const cpp_utils = require('../../cpp_utils/cpp_utils');
const shm = require('shm-typed-array');
const deepmerge = require('deepmerge');
const net = require('net');
const os = require("os");
const _ = require('underscore')._;
const http = require('http');
const fs = require('fs');
const assert = require('assert');
const pgp = require('pg-promise')({
  error: (err, e) => {
    console.error("PGSQL error: ", err, 
      "\n Query: ", e.query, 
      "\n Params: ", e.params,
      "\n Ctx: ", e.ctx
    );
  }
});
const co = require('co');
const child_process = require('child_process');
const EventEmitter = require('events');
const numCPUs = os.cpus().length;
const ProgressBar = require('progress');
const Helpers = require('../Helpers');
const redis = require("redis");
const nodeCleanup = require('node-cleanup');


/**
 * 
 */
class EmfBase extends EventEmitter {
  /**
   * @return array default options
   */
  static get DefaultOptions() {
    return {
      //to be loaded from config*.js
      db: null,
      redis: null,

      dbType: 'ml', //'ml', 'mal'
      maxRating: {
        mal: 10,
        ml: 5,
      },

      // Params for ALS algo
      als: {
        // Regularization term for user latent factors
        userFactReg: 0.05,
        // Regularization term for item latent factors
        itemFactReg: 0.05,
        // When initing factors with random values before calc, 
        //  first U/I factor can be set as avg U/I rating.
        // This approach can minimize RMSE slightly, but prediction values can be > maxRating.
        // To use or not to use? Need to investigate...
        initFirstFactorAsAvgRating: false,
      },
      // Params for SGD algo
      sgd: {
        // Learning rate
        learningRate: 0.01,
        // Regularization term for user latent factors
        userFactReg: 0.0,
        // Regularization term for item latent factors
        itemFactReg: 0.0,
        // Regularization term for user biases
        userBiasReg: 0.0,
        // Regularization term for item biases
        itemBiasReg: 0.0,
      },
      // Number of latent factors to use in matrix factorization model
      factorsCount: 100, //recommended max for mal - 300-400
      // Number of iteration to train model
      trainIters: 10,
      // Method of optimization: "als", "sgd" (deprecated)
      alg: 'als',
      // Distribution (in %) of data set to 1) train 2) validate 3) test prediction
      dataSetDistr: [85, 10, 5],
      ratingsInPortionForRmse: 10*1000,
      ratingsInPortionForSgd: 10*1000,
      ratingsInPortionForAls: {
        // affects on fetch buffer size
        byUser: 10*1000,
        byItem: 10*1000,
      },
      
      // BLAS and LAPACK utilizes all threads by default
      // (Actually it depends on options they are compiled with)
      // Warning! When they are using all threads, 2 processes works much slower that 1 
      //  (because of context switch?)
      // So don't set numThreadsForTrain.als > 1 in this case
      numThreadsForTrain: { als: 1, sgd: 1 }, 
      numThreadsForRmse: numCPUs,
      useDoublePrecision: false,
      usePortionsCache: true,

      // Low RAM mode - keep factors always on disk rather in shared memory
      lowmem: false,
      // Create shm only during training, when retraining - recreate shm and init data from files 
      // (factorsPath)
      // true is faster, but big amount of RAM is always occupied by factors shm
      keepFactorsOpened: true,

      // Take profits of clustering
      useClustering: true,
      // Port to listen in cluster (applies to all nodes - master & slaves)
      clusterServerPort: 7101,
      // Slave-nodes in clustrer will connect to master-node on this host&port
      clusterMasterHost: 'localhost',
      clusterMasterPort: 7101,


      //not configurable! don't change
      shared: {
        userFactorsShmKey: -1,
        itemFactorsShmKey: -1,
        userBiasShmKey: -1,
        itemBiasShmKey: -1,
        portionBufferShmKeys: {},
      },
    };
  };

  static get InitialStats() {
    return {
      //total
      totalUsersCount: -1,
      totalItemsCount: -1,
      totalRatingsAvg: 0,
      //part for current train
      trainUsersCount: -1,
      trainItemsCount: -1,
      trainUsersRatingsCount: 0,
      trainItemsRatingsCount: 0,
      maxRatingsPerUser: 0,
      maxRatingsPerItem: 0,
      ratingsCntPerUser: [],
      ratingsAvgPerUser: [],
      ratingsCntPerItem: [],
      ratingsAvgPerItem: [],
      //distribution
      ratingsCountTrain: 0,
      ratingsCountValidate: 0,
      ratingsCountTest: 0,
      //portions
      portionsCount: {},
      maxRowsInPortion: {},
      maxRatingsInPortion: {},
      portionsRowIdTo: {}
    }
  };

  //
  // Getters, setters
  //
  get TypedArrayKey() {
    return this.options.useDoublePrecision ? 'Float64Array' : 'Float32Array';
  }
  get TypedArrayClass() {
    return this.options.useDoublePrecision ? Float64Array : Float32Array;
  }
  get TypedArraySize1() {
    return this.options.useDoublePrecision ? 8 : 4;
  }
  get factorsWorkingPath() {
    return __dirname + '/../../data/'+this.options.dbType+'_factors_working';
  }
  get factorsReadyPath() {
    return __dirname + '/../../data/'+this.options.dbType+'_factors_ready';
  }
  get factorsTempPath() {
    return __dirname + '/../../data/'+this.options.dbType+'_factors_tmp';
  }
  get factorsPath() {
    return this.isRecommender || !this.options.lowmem ? 
      this.factorsReadyPath : this.factorsWorkingPath;
  }
  /**
   * Statuses:
   * <empty>
   * preparing
   * gathering
   * syncing
   * training
   * ready
   * destroyed
   */
  get status() { return this._status; }
  //sugar getter, setters:
  get factorsCount() { return this.options.factorsCount; }
  set factorsCount(v) { this.options.factorsCount = v; }
  get maxRating() { return this.options.maxRating[this.options.dbType] }
  get totalUsersCount() { return this.stats.totalUsersCount; }
  set totalUsersCount(v) { this.stats.totalUsersCount = v; }
  get totalItemsCount() { return this.stats.totalItemsCount; }
  set totalItemsCount(v) { this.stats.totalItemsCount = v; }
  get trainUsersCount() { return this.stats.trainUsersCount; }
  set trainUsersCount(v) { this.stats.trainUsersCount = v; }
  get trainItemsCount() { return this.stats.trainItemsCount; }
  set trainItemsCount(v) { this.stats.trainItemsCount = v; }
  get portionsCount() { return this.stats.portionsCount; }
  set portionsCount(v) { this.stats.portionsCount = v; }
  get portionsRowIdTo() { return this.stats.portionsRowIdTo; }
  set portionsRowIdTo(v) { this.stats.portionsRowIdTo = v; }
  get maxRowsInPortion() { return this.stats.maxRowsInPortion; }
  set maxRowsInPortion(v) { this.stats.maxRowsInPortion = v; }
  get maxRatingsInPortion() { return this.stats.maxRatingsInPortion; }
  set maxRatingsInPortion(v) { this.stats.maxRatingsInPortion = v; }

  //values for malrec_ratings.dataset_type
  static get DatasetTypeI2K() {
    return {
      0: 'not', //not in split
      1: 'train',
      2: 'validate',
      3: 'test',
      4: 'new', //new, not in split
    };
  }

  static get DatasetTypeK2I() {
    return {
      'not': 0,
      'train': 1,
      'validate': 2,
      'test': 3,
      'new': 4,
    };
  }

  /**
   *
   */
  constructor(isRecommender = false, isClusterMaster = true, 
    workerId = -1, workerProcess = null) {
    super();
    this.process = workerProcess;
    this.workerId = workerId;
    this.isMaster = (workerId == -1 && !isRecommender);
    this.isWorker = !this.isMaster;
    this.isLord = (this.isMaster && isClusterMaster);
    this.isChief = (this.isMaster && !isClusterMaster);
    this.isRecommender = isRecommender;

    this._status = "";
    this.stats = Object.assign({}, cls.InitialStats);

    this.userFactorsFd = null;
    this.itemFactorsFd = null;
    this.userFactors = null;
    this.itemFactors = null;
    this.globalAvgShift = 0;
    this.globalBias = 0;
    this.userBias = null;
    this.itemBias = null;
  };

  /**
   *
   */
  init(config, options = {}) {
    nodeCleanup(() => {
      this.destroy();
    });

    this.options = deepmerge.all([cls.DefaultOptions, config.common, {
      db: config.db,
      redis: config.redis,
    }, config.emf, options]);

    this.userFactorsFilename = 'user_factors';
    this.itemFactorsFilename = 'item_factors';
    this.userBiasFilename = 'user_bias';
    this.itemBiasFilename = 'item_bias';
    this.calcInfoFilename = 'calc_info.json';

    this.userFactorsPath = this.factorsPath + '/' + this.userFactorsFilename;
    this.itemFactorsPath = this.factorsPath + '/' + this.itemFactorsFilename;
    this.userBiasPath = this.factorsPath + '/' + this.userBiasFilename;
    this.itemBiasPath = this.factorsPath + '/' + this.itemBiasFilename;
    this.calcInfoPath = this.factorsPath + '/' + this.calcInfoFilename;

    if (this.options.alg == 'sgd') {
      this.options.useClustering = false;
      this.options.numThreadsForTrain.sgd = 1;
    }
  }

  /**
   *
   */
  checkDbConnection() {
    return this.db.proc('version');
  }
  
  /**
   * @return Promise
   */
  /*abstract*/ prepareToTrain() {
    throw new Exception("abstract");
  }

  /**
   * @return Promise
   */
  /*override*/ initTrainIter() {
    return Promise.resolve();
  }


  /**
   *
   */
  isTrainingAllData() {
    return (this.trainUsersCount == this.totalUsersCount 
      && this.trainItemsCount == this.totalItemsCount);
  }

  /**
   *
   */
  isFirstTrain() {
    return (this.calcCnt == 0);
  }

  /**
   *
   */
  setDbStat(key, val) {
    return this.getDbStat(key).then((oldVal) => {
      if (val !== null) 
        val = ''+val;
      if (oldVal === null) {
        return this.db.query("\
          insert into malrec_stats (key, val) \
          values ( $(key), $(val) ) \
        ", {
          key: key,
          val: val,
        });
      } else {
        return this.db.query("\
          update malrec_stats \
          set val = $(val) \
          where key = $(key) \
        ", {
          key: key,
          val: val,
        });
      }
    });
  }

  /**
   *
   */
  getDbStat(key) {
    return this.db.oneOrNone("\
      select val \
      from malrec_stats \
      where key = $(key) \
    ", {
      key: key
    }).then((row) => {
      if (row === null)
        return null;
      else {
        let num = Number(row.val);
        if (!isNaN(num))
          return num;
        else
          return row.val;
      }
    });
  }

  // -----------------------  factors  -----------------------


  /**
   *
   */
  areSharedFactorsOpened() {
    return (this.options.lowmem ? this.userFactorsFd !== null : this.userFactors !== null);
  }

  /**
   *
   */
  detachSharedFactors() {
    if (this.areSharedFactorsOpened()) {
      if(this.options.lowmem) {
        fs.closeSync(this.userFactorsFd);
        fs.closeSync(this.itemFactorsFd);
        this.userFactorsFd = null;
        this.itemFactorsFd = null;
      } else {
        shm.detach(this.userFactors.data.key);
        shm.detach(this.itemFactors.data.key);
        this.options.shared.userFactorsShmKey = null;
        this.options.shared.itemFactorsShmKey = null;
      }
      this.userFactors = null;
      this.itemFactors = null;

      if (this.options.alg == 'sgd') {
        shm.detach(this.userBias.data.key);
        shm.detach(this.itemBias.data.key);
        this.options.shared.userBiasShmKey = null;
        this.options.shared.itemBiasShmKey = null;
        this.userBias = null;
        this.itemBias = null;
      }
    }
  }

  /**
   * @param bool create - create (for master process) or open (for workers)
   * @param bool recreate - true if factors count changed from last time
   */
  openSharedFactorsFiles(create, recreate = false) {
    if (create) {
      let size = this.totalUsersCount * this.factorsCount * this.TypedArraySize1;
      Helpers.createFile(this.userFactorsPath, size, recreate);
    }
    this.userFactorsFd = fs.openSync(this.userFactorsPath, 'r+');

    if (create) {
      let size = this.totalItemsCount * this.factorsCount * this.TypedArraySize1;
      Helpers.createFile(this.itemFactorsPath, size, recreate);
    }
    this.itemFactorsFd = fs.openSync(this.itemFactorsPath, 'r+');
  }

  /**
   *
   */
  createSharedFactors(recreateFiles = false) {
    if (this.options.lowmem) {
      this.openSharedFactorsFiles(true, recreateFiles);
    } else {
      let userFactorsData = shm.create(this.totalUsersCount * this.factorsCount, 
        this.TypedArrayKey);
      let itemFactorsData = shm.create(this.totalItemsCount * this.factorsCount, 
        this.TypedArrayKey);
      this.options.shared.userFactorsShmKey = userFactorsData.key;
      this.options.shared.itemFactorsShmKey = itemFactorsData.key;
      this.userFactors = new Matrix(userFactorsData, 
        {shape: [this.totalUsersCount, this.factorsCount]});
      this.itemFactors = new Matrix(itemFactorsData, 
        {shape: [this.totalItemsCount, this.factorsCount]});
    }

    // SGD: Create biases
    if (this.options.alg == 'sgd') {
      this.globalBias = this.stats.totalRatingsAvg;
      let userBiasData = shm.create(this.totalUsersCount, this.TypedArrayKey);
      let itemBiasData = shm.create(this.totalItemsCount, this.TypedArrayKey);
      this.options.shared.userBiasShmKey = userBiasData.key;
      this.options.shared.itemBiasShmKey = itemBiasData.key;
      this.userBias = new Vector(userBiasData, {shape: this.totalUsersCount});
      this.itemBias = new Vector(itemBiasData, {shape: this.totalItemsCount});
    }
  }
  
  /**
   *
   */
  openSharedFactors() {
    if (this.options.lowmem) {
      this.openSharedFactorsFiles(false);
    } else {
      let userFactorsData = shm.get(this.options.shared.userFactorsShmKey, this.TypedArrayKey);
      let itemFactorsData = shm.get(this.options.shared.itemFactorsShmKey, this.TypedArrayKey);
      this.userFactors = new Matrix(userFactorsData, 
        {shape: [this.totalUsersCount, this.factorsCount]});
      this.itemFactors = new Matrix(itemFactorsData, 
        {shape: [this.totalItemsCount, this.factorsCount]});
    }

    // SGD: Get shared biases
    if (this.options.alg == 'sgd') {
      this.globalBias = this.stats.totalRatingsAvg;
      let userBiasData = shm.get(this.options.shared.userBiasShmKey, this.TypedArrayKey);
      let itemBiasData = shm.get(this.options.shared.itemBiasShmKey, this.TypedArrayKey);
      this.userBias = new Vector(userBiasData, {shape: this.totalUsersCount});
      this.itemBias = new Vector(itemBiasData, {shape: this.totalItemsCount});
    }
  }

  /**
   * oldUsersCnt, oldItemsCnt - 0 to initial fill randoms for all factors, 
   *  != 0 to fill random only for factors of new users/items
   *
   */
  initSharedFactorsRandom(oldUsersCnt = 0, oldItemsCnt = 0) {
    // Initial random values of factors
    if (this.options.lowmem) {
      this.fillFileRandom(this.userFactorsFd, 
        oldUsersCnt * this.factorsCount * this.TypedArraySize1, 1 / this.factorsCount);
      this.fillFileRandom(this.itemFactorsFd, 
        oldItemsCnt * this.factorsCount * this.TypedArraySize1, 1 / this.factorsCount);
      
      if (this.options.alg == 'als' && this.options.als.initFirstFactorAsAvgRating) {
        let ta = new this.TypedArrayClass(1);
        for (let u = oldUsersCnt ; u < this.totalUsersCount ; u++) {
          let avg = this.stats.ratingsAvgPerUser[u];
          if (avg !== undefined) {
            ta[0] = avg;
            fs.writeSync(this.userFactorsFd, Buffer.from(ta.buffer), 0, 1 * this.TypedArraySize1,
              u * this.factorsCount * this.TypedArraySize1);
          }
        }
        for (let i = oldItemsCnt ; i < this.totalItemsCount ; i++) {
          let avg = this.stats.ratingsAvgPerItem[i];
          if (avg !== undefined) {
            ta[0] = avg;
            fs.writeSync(this.itemFactorsFd, Buffer.from(ta.buffer), 0, 1 * this.TypedArraySize1,
              i * this.factorsCount * this.TypedArraySize1);
          }
        }
      }
    } else {
      if (oldUsersCnt == 0)
       this.userFactors.randomNormal(1 / this.factorsCount);
      else
        for (let u = oldUsersCnt ; u < this.totalUsersCount ; u++) {
          let row = this.userFactors.row(u, false);
          row.randomNormal(1 / this.factorsCount);
        }
      if (oldItemsCnt == 0)
        this.itemFactors.randomNormal(1 / this.factorsCount);
      else
        for (let i = oldItemsCnt ; i < this.totalItemsCount ; i++) {
          let row = this.itemFactors.row(i, false);
          row.randomNormal(1 / this.factorsCount);
        }

      if (this.options.alg == 'als' && this.options.als.initFirstFactorAsAvgRating) {
        for (let u = oldUsersCnt ; u < this.totalUsersCount ; u++) {
          let avg = this.stats.ratingsAvgPerUser[u];
          if (avg !== undefined)
            this.userFactors.set(u, 0, avg);
        }
        for (let i = oldItemsCnt ; i < this.totalItemsCount ; i++) {
          let avg = this.stats.ratingsAvgPerItem[i];
          if (avg !== undefined)
            this.itemFactors.set(i, 0, avg);
        }
      }
    }
  }

  /**
   *
   */
  getLatentFactorsPartData(type, factorsBuffer, firstRowId, rowId) {
    let size1 = this.TypedArraySize1;
    let latentFactors = (type == 'byUser' ? this.userFactors : this.itemFactors);
    let latentFactorsPartData;
    if (this.options.lowmem) {
      // Get part of factors cache to write solve result (no copy)
      latentFactorsPartData = new this.TypedArrayClass( factorsBuffer.buffer,
        (rowId - firstRowId) * this.factorsCount * size1, 1 * this.factorsCount );
    } else {
      // Get part of latentFactors to write solve result (no copy)
      latentFactorsPartData = new this.TypedArrayClass( latentFactors.data.buffer, 
        rowId * this.factorsCount * size1, 1 * this.factorsCount );
    }
    return latentFactorsPartData;
  }

  /**
   *
   */
  copySubFixedFactors(type, subFixedFactorsData, indx) {
    let size1 = this.TypedArraySize1;
    let fixedFactors = (type == 'byUser' ? this.itemFactors : this.userFactors);
    let fixedFactorsFd = (type == 'byUser' ? this.itemFactorsFd : this.userFactorsFd);
    let c, colId, cols = indx.length;
    for (c = 0 ; c < cols ; c++) {
      colId = indx[c];
      if (this.options.lowmem) {
        fs.readSync(fixedFactorsFd, Buffer.from(subFixedFactorsData.buffer), 
          c * this.factorsCount * size1, 1 * this.factorsCount * size1,
          colId * this.factorsCount * size1);
      } else {
        BLAS.BufCopy(
          subFixedFactorsData, c * this.factorsCount * size1, 
          fixedFactors.data, colId * this.factorsCount * size1, 
          1 * this.factorsCount * size1);
      }
    }
  }

  /**
   *
   */
  getReadStreamForFactorsBuffer(type, rowsRange, factorsPortionBuffer) {
    let factorsBuffer;
    if (this.options.lowmem) {
      // copy! factors (because factorsPortionBuffer will be overwritten) 
      //  from shm buffer to new buffer
      factorsBuffer = factorsPortionBuffer.slice(0, rowsRange.cnt * this.factorsCount);
    } else {
      let size1 = this.TypedArraySize1;
      let latentFactors = (type == 'byUser' ? this.userFactors : this.itemFactors);
      // get part of factors shm (no copy)
      factorsBuffer = new this.TypedArrayClass( latentFactors.data.buffer, 
        rowsRange.from * this.factorsCount * size1, 
        rowsRange.cnt * this.factorsCount );
    }
    let readStream = new ReadBufferStream(factorsBuffer, rowsRange);
    return readStream;
  }

  /**
   * 
   */
  getWriteStreamToSaveCalcedFactors(type, rowsRange) {
    let writeStream;
    let size1 = this.TypedArraySize1;
    if (this.options.lowmem) {
      // stream to file
      let latentFactorsPath = (type == 'byUser' ? this.userFactorsPath : this.itemFactorsPath);
      let latentFactorsFd = (type == 'byUser' ? this.userFactorsFd : this.itemFactorsFd);
      writeStream = fs.createWriteStream(latentFactorsPath, {
        fd: latentFactorsFd,
        start: rowsRange.from * this.factorsCount * size1,
        end: (rowsRange.from + rowsRange.cnt) * this.factorsCount * size1,
        autoClose: false,
      });
    } else {
      // get part of factors shm (no copy)
      let latentFactors = (type == 'byUser' ? this.userFactors : this.itemFactors);
      let factorsBuffer = new this.TypedArrayClass( latentFactors.data.buffer, 
          rowsRange.from * this.factorsCount * size1, 
          rowsRange.cnt * this.factorsCount );
      writeStream = new WriteBufferStream(factorsBuffer, rowsRange);
    }
    return writeStream;
  }

  /**
   *
   */
  getFactorsFileReadStreams() {
    return [
      fs.createReadStream(this.userFactorsPath, {
        fd: (this.options.lowmem ? this.userFactorsFd : null),
        start: 0,
        end: this.TypedArraySize1 * this.factorsCount * this.totalUsersCount,
        autoClose: !this.options.lowmem,
      }),
      fs.createReadStream(this.itemFactorsPath, {
        fd: (this.options.lowmem ? this.itemFactorsFd : null),
        start: 0,
        end: this.TypedArraySize1 * this.factorsCount * this.totalItemsCount,
        autoClose: !this.options.lowmem,
      })
    ];
  }

  /**
   *
   */
  getFactorsArrReadStreams() {
    return [
      new ReadBufferStream(this.userFactors.data),
      new ReadBufferStream(this.itemFactors.data)
    ];
  }

  /**
   *
   */
  getFactorsFileWriteStreams(factorsPath = null) {
    if (factorsPath === null)
      factorsPath = this.factorsPath;
    let userFactorsPath = factorsPath + '/' + this.userFactorsFilename;
    let itemFactorsPath = factorsPath + '/' + this.itemFactorsFilename;
    return [
      fs.createWriteStream(userFactorsPath, {
        fd: (this.options.lowmem ? this.userFactorsFd : null),
        start: 0,
        end: this.TypedArraySize1 * this.factorsCount * this.totalUsersCount,
        autoClose: !this.options.lowmem,
      }),
      fs.createWriteStream(itemFactorsPath, {
        fd: (this.options.lowmem ? this.itemFactorsFd : null),
        start: 0,
        end: this.TypedArraySize1 * this.factorsCount * this.totalItemsCount,
        autoClose: !this.options.lowmem,
      })
    ];
  }

  /**
   *
   */
  getFactorsArrWriteStreams() {
    return [
      new WriteBufferStream(this.userFactors.data),
      new WriteBufferStream(this.itemFactors.data)
    ];
  }

  /**
   *
   */
  getFactorsReadStreams() {
    if (this.options.lowmem) {
      return this.getFactorsFileReadStreams();
    } else {
      return this.getFactorsArrReadStreams();
    }
  }

  /**
   *
   */
  getFactorsWriteStreams() {
    if (this.options.lowmem) {
      return this.getFactorsFileWriteStreams();
    } else {
      return this.getFactorsArrWriteStreams();
    }
  }

  /**
   *
   */
  getFactorsByteLength() {
    return this.TypedArraySize1 * 
      (this.totalUsersCount * this.factorsCount + this.totalItemsCount * this.factorsCount);
  }

  /**
   *
   */
  getFactorsRowSync(type, rowId) {
    let vec;
    if (this.options.lowmem) {
      let size1 = this.TypedArraySize1;
      let latentFactorsFd = (type == 'byUser' ? this.userFactorsFd : this.itemFactorsFd);
      let start = rowId * this.factorsCount * this.TypedArraySize1;
      vec = new Vector(null, {
        length: 1 * this.factorsCount,
        type: this.TypedArrayClass
      });
      fs.readSync(latentFactorsFd, Buffer.from(vec.data.buffer), 0, vec.length * size1, start);
    } else {
      let latentFactors = (type == 'byUser' ? this.userFactors : this.itemFactors);
      vec = latentFactors.row(rowId, false);
    }
    return vec;
  }

  /**
   *
   */
  getFactorsRow(type, rowId, callback) {
    if (this.options.lowmem) {
      let size1 = this.TypedArraySize1;
      let latentFactorsFd = (type == 'byUser' ? this.userFactorsFd : this.itemFactorsFd);
      let start = rowId * this.factorsCount * this.TypedArraySize1;
      let vec = new Vector(null, {
        length: 1 * this.factorsCount,
        type: this.TypedArrayClass
      });
      fs.read(latentFactorsFd, Buffer.from(vec.data.buffer), 0, vec.length * size1, start, 
        (err, bytesRead, buffer) => {
        callback(vec);
      });
    } else {
      let latentFactors = (type == 'byUser' ? this.userFactors : this.itemFactors);
      let vec = latentFactors.row(rowId, false);
      callback(vec);
    }
  }

  /**
   *
   */
  setFactorsRowSync(type, rowId, vec) {
    if (this.options.lowmem) {
      let size1 = this.TypedArraySize1;
      let latentFactorsFd = (type == 'byUser' ? this.userFactorsFd : this.itemFactorsFd);
      let start = rowId * this.factorsCount * this.TypedArraySize1;
      fs.writeSync(latentFactorsFd, Buffer.from(vec.data.buffer), 0, vec.length * size1, start);
    } else {
      //see getFactorsRow() - was get by ref, no copy
    }
  }



  // -----------------------  predict  -----------------------

  /**
   *
   */
  getCanPredictError() {
    if (this._status != 'ready') {
      return "Status is not ready";
    } else if (!this.areSharedFactorsOpened()) {
      return "Factors not loaded";
    } else {
      return null;
    }
  }

  /**
   * userId, itemId are 0-based
   */
  predict(userId, itemId, userFactors = null) {
    return this.options.alg == 'als' ? this.alsPredict(userId, itemId, userFactors) :
      this.sgdPredict(userId, itemId, userFactors);
  }

  /**
   * userId, itemId are 0-based
   */
  predictSync(userId, itemId, userFactors = null) {
    return this.options.alg == 'als' ? this.alsPredictSync(userId, itemId, userFactors) :
      this.sgdPredictSync(userId, itemId, userFactors);
  }

  /**
   * userId, itemId are 0-based
   */
  alsPredict(userId, itemId, callback, uF = null) {
    let iF = null;
    let calcIfReady = () => {
      if (uF && iF) {
        callback(this._alsPredict(uF, iF));
      }
    };
    if (!uF) {
      this.getFactorsRow('byUser', userId, (_uF) => {
        uF = _uF;
        calcIfReady();
      });
    }
    this.getFactorsRow('byItem', itemId, (_iF) => {
      iF = _iF;
      calcIfReady();
    });
  }

  /**
   * userId, itemId are 0-based
   */
  alsPredictSync(userId, itemId, uF = null) {
    if (!uF)
      uF = this.getFactorsRowSync('byUser', userId);
    let iF = this.getFactorsRowSync('byItem', itemId);
    return this._alsPredict(uF, iF);
  }

  /**
   * 
   */
  _alsPredict(uF, iF) {
    return uF.dot(iF) + this.globalAvgShift;
  }

  /**
   * userId, itemId are 0-based
   */
  sgdPredict(userId, itemId, callback, uF = null) {
    let iF = null;
    let calcIfReady = () => {
      if (uF && iF) {
        let res = uF.dot(iF) + this.globalAvgShift;
        callback(this._sgdPredict(userId, itemId, uF, iF));
      }
    };
    if (!uF) {
      this.getFactorsRow('byUser', userId, (_uF) => {
        uF = _uF;
        calcIfReady();
      });
    }
    this.getFactorsRow('byItem', itemId, (_iF) => {
      iF = _iF;
      calcIfReady();
    });
  }

  /**
   * userId, itemId are 0-based
   */
  sgdPredictSync(userId, itemId, uF = null) {
    if (!uF)
      uF = this.getFactorsRowSync('byUser', userId);
    let iF = this.getFactorsRowSync('byItem', itemId);
    return this._sgdPredict(userId, itemId, uF, iF);
  }

  /**
   * 
   */
  _sgdPredict(userId, itemId, uF, iF) {
    let pr = this.globalBias + this.userBias.data[userId] + this.itemBias.data[itemId];
    pr += uF.dot(iF);
    if(isNaN(pr)) {
      throw new Error("Some factor became +/-Infinity." 
        + " Please set lower learningRate in options");
    }
    return pr;
  }

  // -----------------------    -----------------------

  /**
   *
   */
  getMemoryUsage() {
    let wPromises = [];
    if (this.workers) {
      for (let w of this.workers) {
        w.emit("getMemoryUsage");
        wPromises.push(new Promise((resolve, reject) => {
          w.once("setMemoryUsage", (data) => {
            resolve(data.mu);
          });
        }));
      }
    }
    return Promise.all(wPromises).then((muArray) => {
      let mu = [];
      mu[0] = shm.getTotalSize();
      mu[1] = process.memoryUsage()['rss'];
      if (this.workers) {
        for (let wi = 0 ; wi < this.workers.length ; wi++) {
          mu[2+wi] = muArray[wi]['rss'];
        }
      }
      return mu;
    });
  }

  /**
   *
   */
  fixPeakMemoryUsage() {
    this.getMemoryUsage().then((mu) => {
      let muTotal = mu.reduce((a, b) => (a + b));
      if (!this.peakMU || muTotal > this.peakMUTotal) {
        this.peakMU = mu;
        this.peakMUTotal = muTotal;
      }
    });
  }

  /**
   *
   */
  formatPeakMemoryUsage() {
    let str = "";
    if (this.peakMU) {
      str = "shm: " + Helpers.humanFileSize(this.peakMU[0]);
      str += "; main: " + Helpers.humanFileSize(this.peakMU[1]);
      if (this.peakMU.length > 2) {
        str += "; workers: ";
        for (let i = 2 ; i < this.peakMU.length ; i++) {
          str += (i > 2 ? ", " : "") + Helpers.humanFileSize(this.peakMU[i]);
        }
      }
    }
    return str;
  }

  /**
   *
   */
  destroy() {
    if (this._status != "destroyed") {
      this._status = "destroyed";
      pgp.end();
      this.detachSharedFactors();

      if (this.isMaster) {
        this.detachWorkPortionBuffers();
        this.killWorkers();
        console.log(this.constructor.name + " destroyed");
        console.log('Peak memory usage: ' + this.formatPeakMemoryUsage());
        this.whenAllWorkersAreDead().then(() => {
          this.emit("destroyed");
        });
      }
    }
  }

  /**
   *
   */
  fillFileRandom(fd, offset = 0, deviation, mean) {
    let bufSize = 1024*1024*10; //10MB
    let bufLength = bufSize / this.TypedArraySize1; //in reals
    let stat = fs.fstatSync(fd);
    let fileSize = stat.size;
    let vect = new Vector(null, {
      length: bufLength, 
      type: this.TypedArrayClass
    });
    let s;
    for (let i = offset ; i < fileSize ; i += bufSize) {
      s = Math.min(bufSize, fileSize - offset - i*bufSize);
      vect.randomNormal(deviation, mean);
      fs.writeSync(fd, Buffer.from(vect.data.buffer), 0, s, i*bufSize);
    }
  }

}
var cls = EmfBase; //for using "cls.A" as like "self::A" inside class

module.exports = {
  EmfBase,

  TcpSocket,
  ReadBufferStream,
  WriteBufferStream,
  vectorious,
  Matrix,
  Vector,
  SpMatrix,
  SpVector,
  BLAS,
  cpp_utils,
  shm,
  deepmerge,
  net,
  os,
  _,
  http,
  fs,
  assert,
  pgp,
  co,
  child_process,
  EventEmitter,
  numCPUs,
  ProgressBar,
  redis,
  nodeCleanup,

  Helpers,
  EmfProcess,
};
