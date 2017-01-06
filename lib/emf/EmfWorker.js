/**
 * Worker process.
 * It's main and only goal is to work with portion of data provided by master 
 *  and send result to master.
 * Calcs latend factors for ALS, 
 *  or latent vectors and biases for SGD, 
 *  or prediction errors for RMSE.
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
class EmfWorker extends EmfBase {

  /**
   *
   */
  constructor(isClusterMaster = true, workerId = -1, workerProcess = null) {
    assert(workerId != -1);
    assert(workerProcess instanceof EmfProcess);
    super(false, isClusterMaster, workerId, workerProcess);
  }

  /**
   *
   */
  init(config, options = {}) {
    super.init(config, options);
    this._status = "ready";

    this.process.on('getMemoryUsage', this.mw_getMemoryUsage.bind(this));
    this.process.on('prepareToTrain', this.mw_prepareToTrain.bind(this));
    this.process.on('startTrain', this.mw_startTrain.bind(this));
    this.process.on('endTrain', this.mw_endTrain.bind(this));
    this.process.on('startTrainStep', this.mw_startTrainStep.bind(this));
    this.process.on('startCalcRmse', this.mw_startCalcRmse.bind(this));
    this.process.on('calcTrainAlsPortion', this.mw_calcTrainAlsPortion.bind(this));
    this.process.on('calcTrainSgdPortion', this.mw_calcTrainSgdPortion.bind(this));
    this.process.on('calcRmsePortion', this.mw_calcRmsePortion.bind(this));

    return Promise.resolve();
  }

  /**
   *
   */
  prepareToTrain() {
    throw new Exception("No manual call. Only by event 'prepareToTrain' from master.");
  }

  /**
   *
   */
  openWorkPortionBuffers() {
    let useForTrain = this.workerId < this.options.numThreadsForTrain[this.options.alg];
    let useForAlsTrain = useForTrain && (this.options.alg == 'als');
    let keys = this.options.shared.portionBufferShmKeys[this.workerId];
    this.portionBuffer = {
      alsVals: useForAlsTrain ? shm.get(keys.alsVals, this.TypedArrayKey) : null,
      alsIndx: useForAlsTrain ? shm.get(keys.alsIndx, 'Int32Array') : null,
      alsRows: useForAlsTrain ? shm.get(keys.alsRows, 'Int32Array') : null,
      singVals: shm.get(keys.singVals, this.TypedArrayKey),
      singIndx: shm.get(keys.singIndx, 'Int32Array'),
      factorsBuffer: useForAlsTrain && this.options.lowmem ? 
        shm.get(keys.factorsBuffer, this.TypedArrayKey) : null,
    };
  }

  /**
   *
   */
  detachWorkPortionBuffers() {
    if (this.portionBuffer) {
      let shmKeys = this.options.shared.portionBufferShmKeys[this.workerId];
      for (let k in shmKeys) {
        if (shmKeys[k] !== null)
          shm.detach(shmKeys[k]);
      }
      this.portionBuffer = null;
      this.options.shared.portionBufferShmKeys[this.workerId] = null;
    }
  }

  /**
   * 
   */
  mw_getMemoryUsage() {
    this.process.emit("setMemoryUsage", {
      mu: process.memoryUsage()
    });
  }

  /**
   * data.stats
   * data.options
   */
  mw_prepareToTrain(data) {
    this._status = "preparing";

    this.stats = data.stats;
    this.options = data.options;

    this.openSharedFactors();
    this.openWorkPortionBuffers();

    this._status = "ready";
    this.process.emit('preparedToTrain');
  }

  /**
   * msg.stepType
   */
  mw_startTrainStep(msg) {
    this.workType = 'train';
    this.stepType = msg.stepType;
  }

  /**
   * msg.stepType
   * msg.globalAvgShift
   */
  mw_startCalcRmse(msg) {
    this.workType = 'rmse';
    this.stepType = msg.stepType;
    this.globalAvgShift = msg.globalAvgShift;
  }

  /**
   *
   */
  mw_startTrain() {
    this._status = "training";
  }

  /**
   *
   */
  mw_endTrain() {
    this.detachWorkPortionBuffers();
    this.detachSharedFactors();
    this._status = "ready";
  }

  /**
   *
   */
  mw_calcTrainAlsPortion(msg) {
    let t1 = Date.now();

    let rowsInPortion = this.portionBuffer.alsRows[0];
    let firstRowId;
    let totalCols = 0;
    if (rowsInPortion > 0) {
      let _lambda = (this.stepType == 'byUser' ? 
        this.options.als.userFactReg : this.options.als.itemFactReg);
      let size1 = this.portionBuffer.alsVals.constructor.BYTES_PER_ELEMENT;
      firstRowId = this.portionBuffer.alsRows[1];
      let  r, 
        cols, rowId, colId, 
        vals, indx, 
        bufOffset = 0,
        _n,
        subFixedFactors;

      // Create matrixes
      let latentFactorsPart, latentFactorsPartData, tmp;
      let maxCols = 0;
      for (r = 0 ; r < rowsInPortion ; r++) {
        cols = this.portionBuffer.alsRows[1 + r*2 + 1];
        totalCols += cols;
        if (cols > maxCols)
          maxCols = cols;
      }
      let A = new Matrix(null, {
        shape: [this.factorsCount, this.factorsCount], 
        type: this.TypedArrayClass 
      });
      let lambda = new Matrix(null, {
        shape: [this.factorsCount, this.factorsCount], 
        type: this.TypedArrayClass 
      });
      subFixedFactors = new Matrix(null, {
        shape: [maxCols, this.factorsCount], 
        type: this.TypedArrayClass 
      });
      let rat;

      for (r = 0 ; r < rowsInPortion ; r++) {
        rowId = this.portionBuffer.alsRows[1 + r*2];
        cols = this.portionBuffer.alsRows[1 + r*2 + 1];
        vals = new this.TypedArrayClass( this.portionBuffer.alsVals.buffer, 
          bufOffset * size1, cols );
        indx = new Int32Array( this.portionBuffer.alsIndx.buffer, bufOffset * 4, cols );
        bufOffset += cols;

        latentFactorsPartData = this.getLatentFactorsPartData(
          this.stepType, this.portionBuffer.factorsBuffer, firstRowId, rowId);
        latentFactorsPart = new Matrix( latentFactorsPartData, 
          { shape: [1, this.factorsCount] } );
        
        // Init matrix subFixedFactors for current row
        subFixedFactors.shape = [ cols, this.factorsCount ];
        this.copySubFixedFactors(this.stepType, subFixedFactors.data, indx);

        // Calc matrix A for current row
        BLAS.gemm(subFixedFactors.data, subFixedFactors.data, A.data, this.factorsCount, 
          this.factorsCount, subFixedFactors.shape[0], BLAS.Trans, BLAS.NoTrans);
        _n = cols;
        lambda.diagonal(_lambda * _n);
        A.add(lambda);

        // Init matrix rat for current row
        rat = new Matrix(vals, {
          shape: [1, cols], 
          type: this.TypedArrayClass 
        });

        subFixedFactors.transposed(); // factorsCount x cols
        rat.shape = [ cols, 1 ]; // transposed: rat - cols x 1
        tmp = subFixedFactors.multiply( rat ); // tmp - factorsCount x 1
        Matrix.solveSquare( A, tmp, tmp );
        tmp.transpose( latentFactorsPart );
      }

      subFixedFactors = null;
    }

    let t2 = Date.now();
    this.process.emit('completedPortion', {
      portionNo: msg.portionNo, 
      rowsRange: {from: firstRowId, cnt: rowsInPortion},
      ratingsInPortion: totalCols,
      time: (t2-t1),
      memoryUsage: this.memoryUsage,
    });
  }

  /**
   *
   */
  mw_calcRmsePortion(msg) {
    let t1 = Date.now();

    let rSumDiff2 = 0, rCnt = 0, rSum = 0;
    let totalRatings = this.portionBuffer.singIndx[0];
    let userId, itemId, ratCheck, ratPredict;
    for (let i = 0 ; i < totalRatings ; i++) {
      userId = this.portionBuffer.singIndx[1 + i*2];
      itemId = this.portionBuffer.singIndx[1 + i*2 + 1];
      ratCheck = this.portionBuffer.singVals[i];
      ratPredict = this.predictSync(userId, itemId);
      rSumDiff2 += Math.pow(ratCheck - ratPredict, 2);
      rSum += ratPredict;
      rCnt++;
    }


    let t2 = Date.now();
    this.process.emit('completedPortion', {
      portionNo: msg.portionNo, 
      time: (t2-t1), 
      memoryUsage: this.memoryUsage,
      rSumDiff2: rSumDiff2,
      rCnt: rCnt,
      rSum: rSum,
    });
  }

  /**
   *
   */
  mw_calcTrainSgdPortion(msg) {
    let t1 = Date.now();

    let userId, itemId, rat, ratPredict, err;
    let totalRatings = this.portionBuffer.singIndx[0];
    for (let i = 0 ; i < totalRatings ; i++) {
      userId = this.portionBuffer.singIndx[1 + i*2];
      itemId = this.portionBuffer.singIndx[1 + i*2 + 1];
      rat = this.portionBuffer.singVals[i];
      ratPredict = this.sgdPredictSync(userId, itemId);
      err = (rat - ratPredict);

      // Update biases
      this.userBias.data[userId] += this.options.sgd.learningRate * 
        (err - this.options.sgd.userBiasReg * this.userBias.data[userId]);
      this.itemBias.data[itemId] += this.options.sgd.learningRate * 
        (err - this.options.sgd.itemBiasReg * this.itemBias.data[itemId]);

      // Update latent factors
      let uFs = this.getFactorsRowSync('byUser', userId);
      let iFs = this.getFactorsRowSync('byItem', itemId);
      for (let f = 0 ; f < this.factorsCount ; f++) {
        uFs.data[f] += this.options.sgd.learningRate * 
          ( err * iFs.data[f] - this.options.sgd.userFactReg * uFs.data[f] );
        iFs.data[f] += this.options.sgd.learningRate * 
          ( err * uFs.data[f] - this.options.sgd.itemFactReg * iFs.data[f] );
      }
      this.setFactorsRowSync('byUser', userId, uFs);
      this.setFactorsRowSync('byItem', itemId, uFs);
    }

    let t2 = Date.now();
    this.process.emit('completedPortion', {
      portionNo: msg.portionNo, time: (t2-t1), 
      time: (t2-t1), 
      memoryUsage: this.memoryUsage, 
    });
  }

}
var cls = EmfWorker; //for using "cls.A" as like "self::A" inside class

module.exports = EmfWorker;

