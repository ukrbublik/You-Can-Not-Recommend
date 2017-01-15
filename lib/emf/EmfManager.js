/**
 * "Manager" - just not worker. 
 * Workers operates with portions, managers can load/save all calculations.
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



/**
 * 
 */
class EmfManager extends EmfBase {

  /**
   *
   */
  constructor(isRecommender = false, isClusterMaster = true) {
    super(isRecommender, isClusterMaster, -1, null);

    this.eventEmitter = new EventEmitter();

    this.calcDate = null;
    this.calcCnt = 0;
    this.lastCalcDate = null;
    this.lastCalcInfo = null;

    if (!this.isRecommender) {
      const EmfRecommender = require('./EmfRecommender');
      this.recommender = new EmfRecommender(isClusterMaster);
    }
  }

  /**
   *
   */
  init(config, options = {}, dbConnection = null, redisConnection = null) {
    super.init(config, options);

    let promises = [];
    if (dbConnection)
      this.db = dbConnection;
    else {
      this.db = pgp(this.options.db);
      promises.push(this.checkDbConnection());
    }

    if (redisConnection)
      this.redis = redisConnection;
    else {
      this.redis = redis.createClient(this.options.redis);
      this.redis.on("error", (err) => {
        console.error("Redis error: ", err);
      });
      promises.push(new Promise((resolve, reject) => {
        this.redis.once("error", (err) => {
          reject(err);
        });
        this.redis.once("connect", () => {
          resolve();
        });
      }));
    }

    return Promise.all(promises).then(() => {
      return this.initCluster();
    }).then(() => {
      return this.isRecommender ? Promise.resolve() : 
        this.recommender.init(config, options, this.db, this.redis);
    }).then(() => {
      return this.loadCalcResults();
    }).then(() => {
      this._status = "ready";
    });
  }


  /**
   *
   */
  /*abstract*/ initCluster() {
    return Promise.resolve();
  }


  // ----------------------- manage calc results  -----------------------

  /**
   *
   */
  getCalcInfo() {
    return {
      //calc options
      alg: this.options.alg,
      algOptions: this.options[this.options.alg],
      useDoublePrecision: this.options.useDoublePrecision,
      factorsCount: this.factorsCount,
      dataSetDistr: this.options.dataSetDistr,
      //data state @ calc start moment
      totalUsersCount: this.totalUsersCount,
      totalItemsCount: this.totalItemsCount,
      dbType: this.options.dbType,
      calcDate: this.calcDate,
      calcCnt: this.calcCnt,
      //results
      globalAvgShift: this.globalAvgShift,
      globalBias: this.globalBias,
    };
  }


  _canReuseCalcResults(ci1) {
    let ci2 = this.getCalcInfo();
    return (ci1 !== null
      && ci1.alg == ci2.alg
      && ci1.dbType == ci2.dbType
      //tip: can use prev generated factors for recommender until new ones are generated
      && (this.isRecommender ? true :
        ci1.factorsCount == ci2.factorsCount 
        && ci1.useDoublePrecision == ci2.useDoublePrecision
        //&& JSON.stringify(ci1.algOptions) == JSON.stringify(ci2.algOptions)
      )
    );
  }

  _didUsersItemsCntsChanged(ci1) {
    let ci2 = this.getCalcInfo();
    return !(ci1 !== null 
      && ci1.totalUsersCount == ci2.totalUsersCount 
      && ci1.totalItemsCount == ci2.totalItemsCount);
  }

  /**
   *
   */
  _loadBiasesFromFiles() {
    let promises = [];

    if (this.options.alg == 'sgd') {
      let readStream, writeStream;
      for (let i = 0 ; i < 2 ; i++) {
        //i: 0 - for users, 1 - for items
        readStream = fs.createReadStream(i == 0 ? this.userBiasPath : this.itemBiasPath);
        writeStream = new WriteBufferStream(i == 0 ? this.userBias.data : this.itemBias.data);
        promises.push(Helpers.pipePromise(readStream, writeStream));
      }
    }

    return Promise.all(promises);
  }

  /**
   *
   */
  _saveBiasesToFiles() {
    let promises = [];

    if (this.options.alg == 'sgd') {
      let readStream, writeStream;
      for (let i = 0 ; i < 2 ; i++) {
        //i: 0 - for users, 1 - for items
        readStream = new ReadBufferStream(i == 0 ? this.userBias.data : this.itemBias.data);
        writeStream = fs.createWriteStream(i == 0 ? this.userBiasPath : this.itemBiasPath);
        promises.push(Helpers.pipePromise(readStream, writeStream));
      }
    }

    return Promise.all(promises);
  }

  /**
   *
   */
  _loadFactorsFromFiles() {
    let promises = [];

    //load factors (for lowmem will be lazy loaded)
    if (!this.options.lowmem) {
      let readStreams = this.getFactorsFileReadStreams();
      let writeStreams = this.getFactorsArrWriteStreams();
      for (let i = 0 ; i < 2 ; i++) {
        //i: 0 - for users, 1 - for items
        promises.push(Helpers.pipePromise(readStreams[i], writeStreams[i]));
      }
    }

    return Promise.all(promises);
  }

  /**
   *
   */
  deleteCalcResultsSync(factorsPath = null) {
    if (factorsPath === null)
      factorsPath = this.factorsPath;
    let filenames = [
      this.calcInfoFilename,
      this.userFactorsFilename,
      this.itemFactorsFilename,
      this.userBiasFilename,
      this.itemBiasFilename
    ];
    for (let filename of filenames) {
      let filePath = factorsPath + '/' + filename;
      if (fs.existsSync(filePath))
        fs.unlinkSync(filePath);
    }
  }
  
  /**
   *
   */
  copyCalcResults(fromPath, toPath) {
    let filenames = [
      //this.calcInfoFilename, //copy last
      this.userFactorsFilename,
      this.itemFactorsFilename,
      this.userBiasFilename,
      this.itemBiasFilename
    ];
    if (!fs.existsSync(toPath))
      fs.mkdirSync(toPath);
    let copyPromises = [];
    for (let filename of filenames) {
      let fromFilePath = fromPath + '/' + filename;
      let toFilePath = toPath + '/' + filename;
      if (fs.existsSync(fromFilePath)) {
        copyPromises.push(Helpers.copyFilePromise(fromFilePath, toFilePath));
      }
    }
    return Promise.all(copyPromises).then(() => {
      let fromFilePath = fromPath + '/' + this.calcInfoFilename;
      let toFilePath = toPath + '/' + this.calcInfoFilename;
      return Helpers.copyFilePromise(fromFilePath, toFilePath);
    });
  }

  /**
   *
   */
  _saveCalcInfo(calcInfo) {
    this.lastCalcInfo = calcInfo;
    this.globalAvgShift = calcInfo.globalAvgShift;
    this.globalBias = calcInfo.globalBias;
    this.lastCalcDate = calcInfo.calcDate;
    this.calcDate = calcInfo.calcDate;
    this.calcCnt = calcInfo.calcCnt;
    this.totalUsersCount = calcInfo.totalUsersCount;
    this.totalItemsCount = calcInfo.totalItemsCount;
    this.factorsCount = calcInfo.factorsCount;
  }

  /**
   * Called in init()
   * @return promise - bool, true if reused
   */
  loadCalcResults() {
    let calcInfo = null;
    if (fs.existsSync(this.calcInfoPath)) {
      let infoStr = fs.readFileSync(this.calcInfoPath);
      calcInfo = JSON.parse(infoStr);
    }

    let canReuse = false;
    if (calcInfo) {
      //tip: for recommender factorsCount can be different from options, 
      // see _canReuseCalcResults()
      canReuse = this._canReuseCalcResults(calcInfo);
    }

    if (!canReuse) {
      if (!this.isRecommender && this.options.lowmem) {
        //this.factorPath == this.options.factorsWorkingPath
        //this.recommender.factorPath == this.options.factorsReadyPath
        if (!fs.existsSync(this.options.factorsWorkingPath))
          fs.mkdirSync(this.options.factorsWorkingPath);
        if (this._canReuseCalcResults(this.recommender.lastCalcInfo)) {
          console.log(this.constructor.name + ": " 
            + "Copying ready calc result to working...");
          return this.copyCalcResults(this.options.factorsReadyPath, 
            this.options.factorsWorkingPath)
          .then(() => {
            return this.loadCalcResults();
          });
        }
      }
    }

    if (canReuse) {
      //save info
      this._saveCalcInfo(calcInfo);
      console.log(this.constructor.name + ": " + "Loaded calc info " + calcInfo.calcDate);

      //load factors
      if (this.options.keepFactorsOpened) {
        this.detachSharedFactors();
        this.createSharedFactors(false);
        return Promise.all([
          this._loadFactorsFromFiles(), 
          this._loadBiasesFromFiles()
        ]).then(() => {
          return true;
        });
      } else {
        return Promise.resolve(true);
      }
    } else {
      if (calcInfo) {
        if (this.isRecommender) {
          //this.factorPath == this.options.factorsReadyPath
          console.log(this.constructor.name + ": " 
            + "Calc options changed, deleting previous ready calc results");
          this.deleteCalcResultsSync();
        } else {
          if (this.options.lowmem) {
            //this.factorPath == this.options.factorsWorkingPath
            console.log(this.constructor.name + ": " 
              + "Calc options changed, deleting previous working calc results");
            this.deleteCalcResultsSync();
          } else {
            console.log(this.constructor.name + ": " 
              + "Calc options changed, can't reuse calc results");
          }
        }
      } else {
        console.log(this.constructor.name + ": " + "Calc info not loaded");
      }
      return Promise.resolve(false);
    }
  }


  /**
   * Only for trainer, not recommender
   * Called in prepareSharedFactors() on train preparing
   * @return promise - [<bool recreated>, <bool extended>]
   */
  _loadSharedFactorsForTrain() {
    if (this.isRecommender)
      throw new Error("Only for trainer");

    let canReuse = this._canReuseCalcResults(this.lastCalcInfo);
    let usersItemsCntsChanged = this._didUsersItemsCntsChanged(this.lastCalcInfo);
    /*
    //todo: зачем эта ф-ия, удалить?
    let beforeReturn = () => {
      if(this.options.lowmem) {
        //if trainig will be interrupted and app crashes, damaged factors will not be 
        // loaded at next app start, see loadCalcResults()
        if (fs.existsSync(this.calcInfoPath))
          fs.unlinkSync(this.calcInfoPath);
      }
    };
    */
    if (!this.lastCalcInfo || !canReuse) {
      //(re)create factors
      if (this.lastCalcInfo && this.options.lowmem) {
        console.log(this.constructor.name + ": " 
          + "Calc options changed, deleting previous working calc results");
        this.deleteCalcResultsSync();
       }
      this.detachSharedFactors();
      this.createSharedFactors(true);
      return Promise.resolve([true, false]);
    } else if(usersItemsCntsChanged) {
      //keep factors and extend them for more users/items
      console.log(this.constructor.name + ": " 
        + "U/I count changed, reusing previous working calc results");
      this.detachSharedFactors();
      this.createSharedFactors(false);
      //tip: for lowmem==false (using shm) need to delete old shm, create bigger one 
      // and load data from file
      return Promise.all([this._loadFactorsFromFiles(), this._loadBiasesFromFiles()])
        .then(() => {
          return [false, true];
        });
    } else {
      //nothing changed
      if (this.options.keepFactorsOpened && this.areSharedFactorsOpened()) {
        return Promise.resolve([false, false]);
      } else {
        this.detachSharedFactors();
        this.createSharedFactors(false);
        return Promise.all([this._loadFactorsFromFiles(), this._loadBiasesFromFiles()])
          .then(() => {
            return [false, false];
          });
      }
    }
  }

  /**
   * Only for trainer, not recommender
   * Called on train end
   */
  saveCalcResults(calcInfo) {
    if (this.isRecommender)
      throw new Error("Only for trainer");

    //biases (unlike factors) are not live saved to files during train with lowmem=1,
    // so save now
    let saveBiasesPromise = this.options.lowmem ? 
      this._saveBiasesToFiles() : Promise.resolve();
    return Promise.all([saveBiasesPromise]).then(() => {
      //save info
      if(this.options.lowmem)
        fs.writeFileSync(this.calcInfoPath, JSON.stringify(calcInfo, null, 2));

      this._saveCalcInfo(calcInfo);
      console.log(this.constructor.name + ": " + "Saved new calc info " + calcInfo.calcDate);
    }).then(() => {
      let readStreams = null;
      if (!this.options.lowmem) {
        readStreams = {};
        readStreams[this.userFactorsFilename] = new ReadBufferStream(this.userFactors.data);
        readStreams[this.itemFactorsFilename] = new ReadBufferStream(this.itemFactors.data);
        if (this.options.alg == 'sgd') {
          readStreams[this.userBiasFilename] = new ReadBufferStream(this.userBias.data);
          readStreams[this.itemBiasFilename] = new ReadBufferStream(this.itemBias.data);
        }
      }
      return this.recommender._saveCalcResultsToRecommender(calcInfo, readStreams);
    }).then(() => {
      if (!this.options.keepFactorsOpened) {
        this.detachSharedFactors();
      }
    });
  }

  /**
   * Only for recommender
   * Called in saveCalcResults() by trainer
   */
  _saveCalcResultsToRecommender(calcInfo, readStreams) {
    if (!this.isRecommender)
      throw new Error("Only for recommender");

    //for lowmem==1:
    //true requires more disk space - /factors_working, /factors_temp, /factors_ready
    //false - only /factors_working, /factors_ready
    let lowmemUseTempDir = false;

    if (readStreams === null && !lowmemUseTemp) { //trainer has lowmem==1
      //critical section - move /factors_working to /factors_ready
      this.deleteCalcResultsSync(this.options.factorsReadyPath);
      if (fs.existsSync(this.options.factorsReadyPath))
        fs.rmdirSync(this.options.factorsReadyPath);        
      fs.renameSync(this.options.factorsWorkingPath, this.options.factorsReadyPath);

      this._saveCalcInfo(calcInfo);
      console.log(this.constructor.name + ": " + "Saved new calc info " + calcInfo.calcDate);

      this.detachSharedFactors();
      this.createSharedFactors(false);
      return Promise.all([
        this._loadFactorsFromFiles(), 
        this._loadBiasesFromFiles()
      ]).then(() => {
        //copy /factors_ready back to /factors_working
        return this.copyCalcResults(this.options.factorsReadyPath, 
          this.options.factorsWorkingPath);
      });
    } else {
      //save calc results to /factors_temp
      if (!fs.existsSync(this.options.factorsTempPath))
        fs.mkdirSync(this.options.factorsTempPath);
      let copyTempPromise;
      if (readStreams === null) { //trainer has lowmem==1
        //copy /factors_working to /factors_temp
        copyTempPromise = this.copyCalcResults(this.options.factorsWorkingPath, 
          this.options.factorsTempPath);
      } else {
        let saveTempPromises = [];
        for (let filename in readStreams) {
          let filePath = this.options.factorsTempPath + '/' + filename;
          let readStream = readStreams[filename];
          let writeStream = fs.createWriteStream(filePath);
          saveTempPromises.push(Helpers.pipePromise(readStream, writeStream));
        }
        copyTempPromise = Promise.all(saveTempPromises).then(() => {
          let calcInfoPath = this.options.factorsTempPath + '/' + this.calcInfoFilename;
          fs.writeFileSync(calcInfoPath, JSON.stringify(calcInfo, null, 2));
        });
      }
      return copyTempPromise.then(() => {
        //critical section - move /factors_temp to /factors_ready
        this.deleteCalcResultsSync(this.options.factorsReadyPath);
        if (fs.existsSync(this.options.factorsReadyPath))
          fs.rmdirSync(this.options.factorsReadyPath);        
        fs.renameSync(this.options.factorsTempPath, this.options.factorsReadyPath);

        this._saveCalcInfo(calcInfo);
        console.log(this.constructor.name + ": " + "Saved new calc info " + calcInfo.calcDate);

        this.detachSharedFactors();
        this.createSharedFactors(false);
        return Promise.all([
          this._loadFactorsFromFiles(), 
          this._loadBiasesFromFiles()
        ]);
      });
    }
  }


}
var cls = EmfManager; //for using "cls.A" as like "self::A" inside class

module.exports = EmfManager;
