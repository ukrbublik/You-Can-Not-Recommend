/**
 * Recommender - can't train, can recommend
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



/**
 * 
 */
class EmfRecommender extends EmfManager {

  /**
   *
   */
  constructor(isClusterMaster = true) {
    super(true, isClusterMaster);
  }

  /**
   *
   */
  init(config, options = {}, dbConnection = null, redisConnection = null) {
    options = Object.assign({}, options, {
      keepFactorsOpened: true,
      lowmem: true, //read factors from files
    });

    return super.init(config, options, dbConnection, redisConnection);
  }

  /**
   *
   */
  initCluster() {
    return Promise.resolve();
  }


}
var CLS = EmfRecommender; //for using "CLS.A" as like "self::A" inside class

module.exports = EmfRecommender;
