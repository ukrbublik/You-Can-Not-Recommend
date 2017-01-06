/**
 * MAL abstract scanner
 * Has basic thick function grabByIds() that manages grabbing in queue 
 *  and safe parallel grabbing from many insances (with help of redis). 
 *
 * @author ukrbublik
 */

const deepmerge = require('deepmerge');
const assert = require('assert');
const _ = require('underscore')._;
const EventEmitter = require('events');
const redis = require("redis");
const bluebird = require("bluebird");
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);
const pgp = require('pg-promise')({
  error: (err, e) => {
    console.error("PGSQL error: ", err, 
      "\n Query: ", e.query, 
      "\n Params: ", e.params,
      "\n Ctx: ", e.ctx
    );
  }
});
const Helpers = require('../Helpers');
const MalDataProvider = require('./MalDataProvider');
const MalApiClient = require('./MalApiClient');
const MalParser = require('./MalParser');
const MalError = require('./MalError');
const MalDataProcesser = require('./MalDataProcesser');


/**
 * 
 */
class MalBaseScanner {
  /**
   * @return array default options
   */
  static get DefaultOptions() {
    return {
      scanner: {
        cntErrorsToStopGrab: 20,
        saveProcessingIdsAfterEvery: 50,
        log: true,
      },
      id: null,
    }
  }

  constructor() {
    this.eventEmitter = new EventEmitter();
    this.processingIds = {}; //see this.grabByIds()
    this.cntProcessedIds = 0;
    this.cntUsefulProcessedIds = 0;
    this.dateStart = null;
  }

  /**
   *
   */
  init(config, options = {}) {
    this.options = deepmerge.all([ cls.DefaultOptions, config, options ]);

    this.processer = new MalDataProcesser();

    if (!this.scannerId)
      this.scannerId = this.genUniqScannerId();
    console.log("["+this.id+"] " + "Scanner id: " + this.scannerId);

    this.redis = redis.createClient(deepmerge.all([this.options.redis, {
      detect_buffers: true,
    }]));
    this.redis.on("error", (err) => {
      console.error("Redis error: ", err);
    });

    this.db = pgp(this.options.db);

    return Promise.all([
      //wait for connection to redis
      new Promise((resolve, reject) => {
        this.redis.once("error", (err) => {
          reject(err);
        });
        this.redis.once("connect", () => {
          resolve();
        });
      }).then(() => {
        return this.redis.send_commandAsync("client", ["setname", this.scannerId]);
      }),
      //connect to db and init processer
      this.checkDbConnection().then(() => {
        return this.processer.init(this.options.processer, this.db);
      }),
    ]);
  }

  /**
   *
   */
  setProvider(id, provider) {
    this.id = id;
    this.provider = provider;
  }

  /**
   *
   */
  genUniqScannerId() {
    function s4() {
      return Math.floor((1 + Math.random()) * 0x10000)
        .toString(16)
        .substring(1);
    }
    return 'mal.scanner_' + new Date().getTime() + '_' + s4() + s4() + s4();
  }

  /**
   *
   */
  getRedisClients() {
    return this.redis.send_commandAsync("client", ["list"]).then((listStr) => {
      let list = listStr.split("\n").filter(str => str != '');
      let clients = [];
      for (let clientStr of list) {
        let client = {};
        let kvs = clientStr.split(" ");
        for (let kv of kvs) {
          let [k, v] = kv.split("=");
          client[k] = v;
        }
        if (Object.keys(client))
          clients.push(client);
      }
      return clients;
    });
  }


  /**
   *
   */
  getAllScanners() {
    return this.getRedisClients().then((clients) => {
      return clients.filter((client) => client.name.match(/mal\.scanner_(\d+)_([0-9a-f]+)/));
    });
  }

  /**
   *
   */
  getAllScannerIds() {
    return this.getAllScanners().then((clients) => {
      let ids = [];
      for (let client of clients) {
        ids.push(client.name);
      }
      return ids;
    });
  }


  /**
   * opts - assoc array with keys:
   *  fetch - fucntion(id, data) returning promise to return object by id and/or data 
   *   (login for users)
   *  process - function(id, obj, data) returning promise to process obj
   *  getNextIds - optional, function(nextId, limit) returning promise to get next ids to 
   *   grab and corresponding data (logins for users), returns {ids: <arr>, data: <obj>}
   *  getDataForIds - function(ids) returning promise to get objects data (login for users) by ids
   *  totalIdsCnt - function to get count of all ids in task (to calc estimate time)
   *  isByListOfIds - bool, true to grab by prepared list of ids (usually in random order)
   *  trackBiggestId - bool, true to find biggest found id and count of not found ids after it, 
   *   see saveBiggestId()
   *  getListOfIds (for isByListOfIds==true) - function returning promise to get ids to grab
   *  startFromId (for isByListOfIds==false) - int
   *  approxMaxId (for isByListOfIds==false) - int
   *  maxNotFoundIdsToStop (for isByListOfIds==false) - int
   */
  grabByIds(opts) {
    if (!this.provider)
      throw new Error("Scanner hasn't provider!");
    let grabCompleteEventKey = 'grabComplete_' + opts.key;
    let grabStopEventKey = 'grabStop_' + opts.key;

    let grabIterKey = 'mal.' + opts.key + '.grabIter';
    let dateIterStartKey = 'mal.' + opts.key + '.dateIterStart';
    let dateIterEndKey = 'mal.' + opts.key + '.dateIterEnd';
    let nextIndKey = 'mal.' + opts.key + '.nextInd';
    let listOfIdsKey = 'mal.' + opts.key + '.listOfIds';
    let errorIdsKey = 'mal.' + opts.key + '.errorIds';
    let retryIdsKey = 'mal.' + opts.key + '.retryIds';
    let processingIdsKey = 'mal.' + opts.key + '.processingIds';
    let biggestFoundIdKey = 'mal.' + opts.key + '.biggestFoundId';
    let notFoundIdsAfterBiggestKey = 'mal.' + opts.key + '.notFoundAfterBiggestIds';
    let cntSuccessIdsKey = 'mal.' + opts.key + '.cntSuccessIds';
    let cntNotFoundIdsKey = 'mal.' + opts.key + '.cntNotFoundIds';
    let cntTotalIdsKey = 'mal.' + opts.key + '.cntTotalIds';

    if (this.processingIds[opts.key] === undefined)
      this.processingIds[opts.key] = [];
    let processingIds = this.processingIds[opts.key];
    this.cntProcessedIds = 0; //actially processed + not found + failed to fetch
    this.cntUsefulProcessedIds = 0; //only processed + not found, without failed
    let errorIds = [];
    let errs = {};
    let needToSaveErrorIds = false; //flag, there are new failed ids to save to redis
    let incrCntSuccessIds = 0; //cnt of new processed ids, to be incremented to cntSuccessIdsKey
    let incrCntNotFoundIds = 0;
    let grabIter = null; //grabbing iteration
    //null - iter completed, -1 - all ids has been taken by scanners, but not yet comleted,
    // 0.. - next id to pick (for isByListOfIds - next index of listOfIds tp pick)
    let nextInd = null;
    //if true, all ids for grabbing are picked by scanners    
    let shouldStopBecauseNoData = false;
    //if true, scanner should not pick more ids and stop scanning after queue is drained
    let shouldStopBecauseErrors = false;
    //for opt.isByListOfIds list of all ids to scan during current iter by random order
    let listOfIds = null;
    //next 2 vars - see saveBiggestId()
    let biggestFoundId = 0;
    let cntNotFoundAfterBiggestId = 0;
    //will be set after every regrabLost() to know if there are more lost ids
    let hasMoreLostIdsToGrab = true;
    //next 3 vars - flags for exclusive execution of corresponding funcs
    let grabbingNext = false;
    let savingBiggestId = false;
    let waitingForConcurrentScanners = false;
    let dateIterStart;
    let dateIterEnd;
    this.dateStart = new Date();

    //Start new iter or get already started (grabIter)
    //Returns true if started/continued task, else false
    let startGrab = (retry = -1) => {
      this.redis.watch(grabIterKey);
      this.redis.watch(nextIndKey);
      this.redis.watch(listOfIdsKey);
      this.redis.watch(retryIdsKey);
      return this.redis.mgetAsync([ nextIndKey, grabIterKey, 
        dateIterStartKey, dateIterEndKey, retryIdsKey, new Buffer(listOfIdsKey)
      ]).then(([rNextInd, rGrabIter, rDateIterStart, rDateIterEnd, rRetryIds, rListOfIds]) => {
        if (rNextInd !== null)        rNextInd = parseInt(rNextInd.toString());
        if (rGrabIter !== null)       rGrabIter = parseInt(rGrabIter.toString());
        if (rDateIterStart !== null)  rDateIterStart = parseInt(rDateIterStart.toString());
        if (rDateIterEnd !== null)    rDateIterEnd = parseInt(rDateIterEnd.toString());
        if (rRetryIds !== null)
          rRetryIds = rRetryIds.toString().split(',').map(Number);

        if (rNextInd === null) {
          if (rRetryIds && rRetryIds.length) {
            //start new "retry" iter - only retry ids failed in prev iter
            let multi = this.redis.multi();
            opts.isByListOfIds = true;
            nextInd = 0;
            grabIter = rGrabIter + 1;
            multi.set(nextIndKey, nextInd);
            multi.set(grabIterKey, grabIter);
            dateIterStart = new Date();
            multi.set(dateIterStartKey, dateIterStart.getTime());
            multi.del(dateIterEndKey);
            return Promise.resolve().then(() => {
              listOfIds = new Int32Array(rRetryIds);
              let rIdsBuf = Buffer.from(listOfIds.buffer);
              multi.mset(new Buffer(listOfIdsKey), rIdsBuf);
              multi.del(retryIdsKey);
              return multi.execAsync().then((replies) => {
                if (replies === null) {
                  return startGrab(retry + 1); //retry
                } else {
                  console.log("[" + this.id + "] " + "* Starting task " + opts.key 
                    + ", RETRY iter " + grabIter 
                    + " (" + listOfIds.length + " ids in task)");
                  return true;
                }
              });
            });
          } else {
            //start new iter
            //if prev iter was < 5min ago, skip
            if (rDateIterEnd && (new Date().getTime() - rDateIterEnd) < 1000*60*5)
              return Promise.resolve(false);
            else return Promise.resolve().then(opts.totalIdsCnt).then((totalIdsCnt) => {
              let multi = this.redis.multi();
              nextInd = opts.startFromId ? opts.startFromId : 0;
              grabIter = (rGrabIter === null ? 0 : rGrabIter + 1);
              multi.set(cntTotalIdsKey, totalIdsCnt);
              multi.set(nextIndKey, nextInd);
              multi.set(grabIterKey, grabIter);
              dateIterStart = new Date();
              multi.set(dateIterStartKey, dateIterStart.getTime());
              multi.del(dateIterEndKey);
              multi.set(cntSuccessIdsKey, 0);
              multi.set(cntNotFoundIdsKey, 0);
              multi.del(biggestFoundIdKey);
              multi.del(notFoundIdsAfterBiggestKey);
              if (opts.isByListOfIds) {
                return Promise.resolve().then(() => opts.getListOfIds()).then((rIds) => {
                  listOfIds = new Int32Array(rIds);
                  let rIdsBuf = Buffer.from(listOfIds.buffer);
                  multi.mset(new Buffer(listOfIdsKey), rIdsBuf);
                  return multi.execAsync().then((replies) => {
                    if (replies === null) {
                      return startGrab(retry + 1); //retry
                    } else {
                      console.log("[" + this.id + "] " + "* Starting task " + opts.key 
                        + ", iter " + grabIter 
                        + " (" + listOfIds.length + " ids in task)");
                      return true;
                    }
                  });
                });
              } else {
                return multi.execAsync().then((replies) => {
                  if (replies === null) {
                    return startGrab(retry + 1); //retry
                  } else {
                    console.log("[" + this.id + "] " + "* Starting task " + opts.key 
                      + ", iter " + grabIter);
                    return true;
                  }
                });
              }
            });
          }
        } else {
          //iter already started (by other scanner instance), continue it
          return this.redis.hgetAsync(errorIdsKey, this.scannerId).then((rErrorIds) => {
            if (rErrorIds !== null)
              errorIds = rErrorIds.split(',').map(Number);
            grabIter = rGrabIter;
            nextInd = rNextInd;
            dateIterStart = new Date(rDateIterStart);
            if (rListOfIds !== null) {
              opts.isByListOfIds = true;
              listOfIds = new Int32Array(rListOfIds.length / 4);
              rListOfIds.copy(Buffer.from(listOfIds.buffer));
            }
            if (this.options.scanner.log || 1)
              console.log("[" + this.id + "] " + "* Continue task " + opts.key 
                + ", iter " + grabIter);
            return true;
          });
        }
      });
    };

    //Performs regrabLost() or grabNext()
    //Return count of picked ids or -1 if already in process of picking lost ids/next portion
    let grabLostOrNext = () => {
      if (grabbingNext)
        return Promise.resolve(-1);
      grabbingNext = true;
      let pr;
      if (hasMoreLostIdsToGrab) {
        pr = regrabLost().then(([pickedLostIdsCnt, hasMoreLostIds]) => {
          if (pickedLostIdsCnt)
            return pickedLostIdsCnt;
          else
            return grabNext();
        });
      } else {
        pr = grabNext();
      }
      return pr.then((pickedIdsCnt) => {
        if (!pickedIdsCnt)
          shouldStopBecauseNoData = true;
        grabbingNext = false;
        return pickedIdsCnt;
      });
    };

    //If some scanner instance has picked ids and crashed during processing, these ids will be lost.
    //This method will detect those, repick to current scanner and add to queue.
    //Return [<number of repicked ids>, <are there more lost ids?>]
    let regrabLost = () => {
      return getLostIds().then(([busyScannerIds, lostIds, hasMoreLostIds]) => {
        hasMoreLostIdsToGrab = hasMoreLostIds;
        if (lostIds.length) {
          console.log("[" + this.id + "] " + "Found " + lostIds.length + " lost ids");
          return Promise.resolve().then(() => opts.getDataForIds(lostIds)).then((data) => {
            grabIds(lostIds, data);
            return [lostIds.length, hasMoreLostIds];
          });
        } else 
          return [lostIds.length, hasMoreLostIds];
      });
    };

    //Ask redis to safe pick next portion ids, then add ids to queue. 
    //Return number of picked ids (if 0 - no more portions to pick, need to complete iter)
    let grabNext = (retry = -1) => {
      if (nextInd === null || nextInd == -1)
        return Promise.resolve(0); //shouldStopBecauseNoData will be set to true

      this.redis.watch(grabIterKey);
      this.redis.watch(nextIndKey);
      return this.redis.mgetAsync([nextIndKey, grabIterKey])
        .then(([rNextInd, rGrabIter]) => {
          if (rGrabIter != grabIter || rNextInd === null || rNextInd == -1) {
            //if (this.options.scanner.log)
            //  console.log("[" + this.id + "] " + "No more ids to pick" 
            //    + " (another scanner instance has picked last portion)");
            return 0;
          }
          nextInd = parseInt(rNextInd);
          let getResPromise;
          //pick next 'queueSizeConcurrent' ids for portion
          let limit = this.provider.options.queueSizeConcurrent;
          if (!opts.isByListOfIds) {
            let breakScan = opts.maxNotFoundIdsToStop > 0 
              && (!biggestFoundId || biggestFoundId >= opts.approxMaxId)
              && cntNotFoundAfterBiggestId >= opts.maxNotFoundIdsToStop;
            if (breakScan) {
              getResPromise = Promise.resolve({ ids: [] });
            } else {
              if (opts.getNextIds) {
                getResPromise = Promise.resolve()
                  .then(() => opts.getNextIds(nextInd, limit));
              } else {
                let ids = Array.from({length: limit}, 
                  (v, k) => nextInd + k);
                getResPromise = Promise.resolve({ ids: ids });
              }
            }
          } else {
            let newNextInd = Math.min(listOfIds.length, nextInd + limit);
            getResPromise = Promise.resolve({
              ids: Array.from(listOfIds.slice(nextInd, newNextInd)),
              next: (newNextInd == nextInd ? -1 : newNextInd)
            });
          }

          return getResPromise.then((res) => {
            //newNextInd will be -1 if all ids are picked in this iter
            let newNextInd = res.next !== undefined ? res.next 
              : (res.ids.length ? res.ids[res.ids.length - 1] + 1 : -1);
            let multi = this.redis.multi();
            let totalProcessingIds = processingIds.concat(res.ids);
            if (totalProcessingIds.length == 0)
              multi.hdel(processingIdsKey, this.scannerId);
            else
              multi.hset(processingIdsKey, this.scannerId, totalProcessingIds.join(','));
            multi.set(nextIndKey, newNextInd);

            //Also with processingIds save errorIds & success/not found cnts.
            // They must be in 1 transaction! Also see saveCurrentProcessingIds()
            let _incrCntSuccessIds = incrCntSuccessIds, _incrCntNotFoundIds = incrCntNotFoundIds;
            if (incrCntSuccessIds)
              multi.incrby(cntSuccessIdsKey, incrCntSuccessIds);
            if (incrCntNotFoundIds)
              multi.incrby(cntNotFoundIdsKey, incrCntNotFoundIds);
            let _needToSaveErrorIds = needToSaveErrorIds;
            needToSaveErrorIds = false;
            if (_needToSaveErrorIds && errorIds.length)
              multi.hset(errorIdsKey, this.scannerId, errorIds.join(','));
            return multi.execAsync().then((replies) => {
              if (replies === null) {
                if (_needToSaveErrorIds)
                  needToSaveErrorIds = true;
                return grabNext(retry + 1); //retry
              } else {
                incrCntSuccessIds -= _incrCntSuccessIds;
                incrCntNotFoundIds -= _incrCntNotFoundIds;
                processingIds = processingIds.concat(res.ids);
                if (newNextInd == -1)
                  console.log("[" + this.id + "] " + "No more ids to pick");
                nextInd = newNextInd;
                if (res.ids.length > 0) {
                  //get corresponding data
                  let getDataPromise = Promise.resolve().then(() => {
                    return res.data ? res.data : opts.getDataForIds(res.ids);
                  });
                  getDataPromise.then((data) => {
                    grabIds(res.ids, data);
                  });
                }
                return res.ids.length;
              }
            });
          });
        });
    };

    //After picking new portion ids (or lost ids) we need to add them to queue for processing
    let grabIds = (ids, datas) => {
      //if (this.options.scanner.log)
      //  console.log("[" + this.id + "] " + "Pushing ids: " + ids.join(', '));
      for (let id of ids) {
        id = parseInt(id);
        let data = datas ? datas[id] : null;
        let idFound, isError = false;
        opts.fetch(id, data).catch((err) => {
          if (err instanceof MalError && err.errCode == 2) {
            return null;
          } else throw err;
        }).then((obj) => {
          let saveBiggestIdPromise = Promise.resolve();
          idFound = (obj !== null);
          if (opts.trackBiggestId) {
            if (!idFound) {
              if (id > biggestFoundId) {
                saveBiggestIdPromise = saveBiggestId(id, false);
              }
            } else {
              if (id > biggestFoundId) {
                biggestFoundId = id;
                saveBiggestIdPromise = saveBiggestId(id, true);
              }
            }
          }
          let processPromise = opts.process(id, obj, data);
          return Promise.all([saveBiggestIdPromise, processPromise]);
        }).catch((err) => {
          isError = true;
          return err;
        }).then((res) => {
          this.cntProcessedIds++;
          if (isError) {
            let err = res;
            errorIds.push(id);
            errs[id] = err;
            needToSaveErrorIds = true;
          } else {
            this.cntUsefulProcessedIds++;
            if (idFound)
              incrCntSuccessIds++;
            else
              incrCntNotFoundIds++;
          }
          assert(processingIds.indexOf(id) != -1);
          processingIds.splice(processingIds.indexOf(id), 1);
          if (Object.keys(errs).length > this.options.scanner.cntErrorsToStopGrab)
            shouldStopBecauseErrors = true;
          //if (this.options.scanner.log)
          //  console.log("[" + this.id + "] " + (!isError ? 
          //    (idFound ? "Success" : "Not found") : "Fail") + " id: " + id);

          let pr = Promise.resolve();
          if ((this.cntProcessedIds % this.options.scanner.saveProcessingIdsAfterEvery == 0)
            || processingIds.length == 0) {
            pr = saveCurrentProcessingIds();
          }
          pr.then(() => {
            checkIfShouldStop();

            if (!shouldStopBecauseErrors && !shouldStopBecauseNoData
             && (this.cntUsefulProcessedIds > 0 && this.provider.canAddMoreToQueue() 
              || processingIds.length == 0)) {
              grabLostOrNext().then(() => {
                checkIfShouldStop();
              });
            }
          });
        });
      }
    };


    //To allow detecting lost ids from crashed scanners
    // save frequently current processing ids + cnts of success/not found ids + error ids
    let saveCurrentProcessingIds = (retry = -1) => {
      let multi = this.redis.multi();
      let _incrCntSuccessIds = incrCntSuccessIds, _incrCntNotFoundIds = incrCntNotFoundIds;
      if (incrCntSuccessIds)
        multi.incrby(cntSuccessIdsKey, incrCntSuccessIds);
      if (incrCntNotFoundIds)
        multi.incrby(cntNotFoundIdsKey, incrCntNotFoundIds);
      if (processingIds.length == 0) {
        multi.hdel(processingIdsKey, this.scannerId);
      } else {
        multi.hset(processingIdsKey, this.scannerId, processingIds.join(','));
      }
      let _needToSaveErrorIds = needToSaveErrorIds;
      needToSaveErrorIds = false;
      if (_needToSaveErrorIds && errorIds.length) {
        multi.hset(errorIdsKey, this.scannerId, errorIds.join(','));
      }
      return multi.execAsync().then((replies) => {
        if (replies === null) {
          if (_needToSaveErrorIds)
            needToSaveErrorIds = true;
          return saveCurrentProcessingIds(retry + 1); //retry
        } else {
          incrCntSuccessIds -= _incrCntSuccessIds;
          incrCntNotFoundIds -= _incrCntNotFoundIds;
        }
      });
    }

    //If no more ids to grab, wait for all scanners to complete this iter, then emit complete event.
    //If too much errors, stop grabbing and emit error event.
    let checkIfShouldStop = () => {
      if (processingIds.length == 0) {
        if (shouldStopBecauseNoData) {
          waitForConcurrentScanners().then((foundLostIds) => {
            if (foundLostIds === false) {
              //done!
              this.eventEmitter.emit(grabCompleteEventKey);
            }
          });
        } else if (shouldStopBecauseErrors) {
          //stop!
          this.eventEmitter.emit(grabStopEventKey);
        }
      }
    };

    //When grabbing ids by order (!opt.isByListOfIds) and we don't know last id to stop 
    // (users ids), stop after N x 404 fails
    //! Note that if grabbing with many providers in parallel, such detecting is not accurate
    // comparing with simple grabbing by 1 provider
    let saveBiggestId = (id, idFound, retry = -1) => {
      if (retry == -1 && savingBiggestId) {
        return Helpers.promiseWait(() => !savingBiggestId, 10, () => saveBiggestId(id, idFound));
      }

      savingBiggestId = true;
      this.redis.watch(biggestFoundIdKey);
      this.redis.watch(notFoundIdsAfterBiggestKey);
      return this.redis.mgetAsync([biggestFoundIdKey, notFoundIdsAfterBiggestKey, grabIterKey])
        .then(([rBiggestFoundId, rNotFoundIdsAfterBiggest, rGrabIter]) => {
          rBiggestFoundId = rBiggestFoundId === null ? 0 : parseInt(rBiggestFoundId);
          rNotFoundIdsAfterBiggest = !rNotFoundIdsAfterBiggest ? [] 
            : rNotFoundIdsAfterBiggest.split(',').map(Number);
          if (grabIter != rGrabIter)
            return;

          if (idFound) {
            if (id > rBiggestFoundId) {
              let multi = this.redis.multi();
              rNotFoundIdsAfterBiggest = rNotFoundIdsAfterBiggest
                .filter((_id) => (_id > id));
              cntNotFoundAfterBiggestId = rNotFoundIdsAfterBiggest.length;
              multi.set(biggestFoundIdKey, id);
              multi.set(notFoundIdsAfterBiggestKey, rNotFoundIdsAfterBiggest.join(','));
              return multi.execAsync().then((replies) => {
                if (replies === null) {
                  return saveBiggestId(id, idFound, retry + 1); //retry
                }
              });
            } else {
              biggestFoundId = rBiggestFoundId;
              cntNotFoundAfterBiggestId = rNotFoundIdsAfterBiggest.length;
            }
          } else {
            if (rBiggestFoundId > biggestFoundId) {
              biggestFoundId = rBiggestFoundId;
              cntNotFoundAfterBiggestId = rNotFoundIdsAfterBiggest.length;
            }
            if (id > rBiggestFoundId) {
              let multi = this.redis.multi();
              rNotFoundIdsAfterBiggest.push(id);
              cntNotFoundAfterBiggestId = rNotFoundIdsAfterBiggest.length;
              multi.set(notFoundIdsAfterBiggestKey, rNotFoundIdsAfterBiggest.join(','));
              return multi.execAsync().then((replies) => {
                if (replies === null) {
                  return saveBiggestId(id, idFound, retry + 1); //retry
                }
              });
            }
          }
        }).then(() => {
          savingBiggestId = false;
        });
    };

    //Returns bool <found lost ids?>
    //If true, there will be more ids in queue to process, so need to check again later
    //If false, it's safe to complete iter
    //If null, already waiting
    let waitForConcurrentScanners = (isFromTimer = false) => {
      if (!isFromTimer && waitingForConcurrentScanners)
        return Promise.resolve(null);
      waitingForConcurrentScanners = true;
      return getLostIds().then(([busyScannerIds, lostIds, hasMoreLostIds]) => {
        if (lostIds.length) {
          //see regrabLost()
          console.log("[" + this.id + "] " + "Found " + lostIds.length + " lost ids..");
          return Promise.resolve().then(() => 
            opts.getDataForIds(lostIds)
          ).then((data) => {
            grabIds(lostIds, data);
            return true;
          });
        } else if(busyScannerIds.length) {
          //console.log("[" + this.id + "] " + "Busy scanners: " + busyScannerIds.length);
          if (isFromTimer)
            return null; //special value, means "still busy, keep timer running"
          return new Promise((resolve, reject) => {
            let timer;
            let runTimer = () => {
              timer = setTimeout(() => {
                waitForConcurrentScanners(true)
                  .then((foundLostIds) => {
                    if (foundLostIds === null) {
                      clearTimeout(timer);
                      runTimer();
                    } else 
                      resolve(foundLostIds);
                  })
                  .catch((err) => {
                    reject(err);
                  });
              }, 1000);
            };
            runTimer();
          });
        } else return false;
      }).then((res) => {
        waitingForConcurrentScanners = false;
        return res;
      });
    };

    //If some scanner instance has picked ids and crashed during processing, these ids will be lost.
    //This method will detect those, repick to current scanner.
    //Also get number of scanners working on current iter (if 0, can complete iter)
    let getLostIds = (retry = -1) =>  {
      this.redis.watch(processingIdsKey);
      return Promise.all([
        this.getAllScannerIds(),
        this.redis.hkeysAsync(processingIdsKey)
      ]).then(([aliveScannerIds, workingScannerIds]) => {
        let lostScannerIds = _.difference(workingScannerIds, aliveScannerIds);
        let busyScannerIds = _.difference(workingScannerIds, lostScannerIds);
        if (lostScannerIds.length) {
          return this.redis.hmgetAsync(processingIdsKey, lostScannerIds)
          .then((lostScannersItemIds) => {
            let multi = this.redis.multi();
            let pickedLostItemIds = [];
            let limit = this.provider.options.queueSizeConcurrent;
            let i;
            for (i = 0 ; i < lostScannerIds.length ; i++) {
              let scannerId = lostScannerIds[i];
              let lostItemIds = lostScannersItemIds[i];
              if (lostItemIds !== null) {
                let itemIds = lostItemIds.split(',').map(Number).filter((v) => (v >= 0));
                pickedLostItemIds = pickedLostItemIds.concat(itemIds);
                multi.hdel(processingIdsKey, scannerId);
              }
              if (pickedLostItemIds.length >= limit)
                break;
            }
            let hasMoreLostIds = (i < lostScannerIds.length);
            let totalProcessingIds = processingIds.concat(pickedLostItemIds);
            multi.hset(processingIdsKey, this.scannerId, totalProcessingIds.join(','));
            if (busyScannerIds.indexOf(this.scannerId) == -1)
              busyScannerIds.push(this.scannerId);

            //Also with processingIds save errorIds & success/not found cnts - must be in 1 trx,
            // see saveCurrentProcessingIds()
            let _incrCntSuccessIds = incrCntSuccessIds, _incrCntNotFoundIds = incrCntNotFoundIds;
            if (incrCntSuccessIds)
              multi.incrby(cntSuccessIdsKey, incrCntSuccessIds);
            if (incrCntNotFoundIds)
              multi.incrby(cntNotFoundIdsKey, incrCntNotFoundIds);
            let _needToSaveErrorIds = needToSaveErrorIds;
            needToSaveErrorIds = false;
            if (_needToSaveErrorIds && errorIds.length)
              multi.hset(errorIdsKey, this.scannerId, errorIds.join(','));

            return multi.execAsync().then((replies) => {
              if (replies === null) {
                if (_needToSaveErrorIds)
                  needToSaveErrorIds = true;
                return getLostIds(retry + 1); //retry
              } else {
                incrCntSuccessIds -= _incrCntSuccessIds;
                incrCntNotFoundIds -= _incrCntNotFoundIds;
                processingIds = processingIds.concat(pickedLostItemIds);
                return [busyScannerIds, pickedLostItemIds, hasMoreLostIds];
              }
            });
          });
        } else return [busyScannerIds, [], false];
      })
    };

    //When iter is stopped because of errors
    let stopIter = (retry = -1) => {
      return Promise.resolve({
        stopped: true, 
        errs: errs, 
      });
    };

    //When iter is completed, clear corresponding redis items, save error ids to retry
    let completeIter = (retry = -1) => {
      this.redis.watch(errorIdsKey);
      this.redis.watch(retryIdsKey);
      this.redis.watch(nextIndKey);
      this.redis.watch(dateIterEndKey);
      return this.redis.hgetallAsync(errorIdsKey).then((rErrorIds) => {
        return this.redis.getAsync(nextIndKey).then((rNextInd) => {
          let allErrIds = rErrorIds === null ? null : _.uniq(Object.keys(rErrorIds)
            .map((k) => rErrorIds[k].split(','))
            .reduce((prev, curr) => prev.concat(curr))
            .map(Number)
            .filter((v) => (v >= 0))
          ).sort((a, b) => (a - b));
          let isAlreadyCompleted = (rNextInd === null);
          let promise;
          if (!isAlreadyCompleted) {
            let multi = this.redis.multi();
            dateIterEnd = new Date();
            multi.del(nextIndKey);
            multi.del(listOfIdsKey);
            multi.del(errorIdsKey);
            if (allErrIds && allErrIds.length)
              multi.set(retryIdsKey, allErrIds.join(','));
            else
              multi.del(retryIdsKey);
            multi.del(processingIdsKey);
            multi.set(dateIterEndKey, dateIterEnd.getTime());
            promise = multi.execAsync();
          } else {
            promise = Promise.resolve(true);
          }
          return promise.then((replies) => {
            if (replies === null) {
              return completeIter(retry + 1);
            } else {
              return this.redis.mgetAsync([retryIdsKey, cntSuccessIdsKey, cntNotFoundIdsKey, 
                notFoundIdsAfterBiggestKey, biggestFoundIdKey])
              .then(([rRetryIds, rCntSuccessIds, rCntNotFoundIds, rNotFoundIdsAfterBiggest,
                rBiggestFoundId]) => {
                if (rRetryIds !== null)
                  rRetryIds = rRetryIds.split(',');
                if (rNotFoundIdsAfterBiggest !== null)
                  rNotFoundIdsAfterBiggest = rNotFoundIdsAfterBiggest.split(',');
                let cntNotFoundIdsAfterBiggest = rNotFoundIdsAfterBiggest ? 
                  rNotFoundIdsAfterBiggest.length : 0;
                rCntSuccessIds = rCntSuccessIds === null ? 0 : parseInt(rCntSuccessIds);
                rCntNotFoundIds = rCntNotFoundIds === null ? 0 : parseInt(rCntNotFoundIds);
                rBiggestFoundId = rBiggestFoundId === null ? 0 : parseInt(rBiggestFoundId);
                let cntRetryIds = rRetryIds ? rRetryIds.length : 0;
                //if (rRetryIds)
                //  console.log("[" + this.id + "] " + "All error ids to retry: " 
                //    + rRetryIds.join(', '));

                let res = {
                  stopped: false,
                  errs: errs,
                  cntSuccessIds: rCntSuccessIds,
                  cntNotFoundIds: rCntNotFoundIds,
                  cntRetryIds: cntRetryIds,
                };
                if (opts.trackBiggestId) {
                  res.cntNotFoundIdsAfterBiggest = cntNotFoundIdsAfterBiggest;
                  res.biggestFoundId = rBiggestFoundId;
                }
                return Promise.resolve(res);
              });
            }
          });
        });
      });
    };

    //Determine best queue concurrent size (depends on task + provider combo)
    let queueSizeConcurrent = this.provider.options.queueSizeConcurrent, 
      parserQueueSizeConcurrent = this.provider.options.parserQueueSizeConcurrent;
    let providerOptions = this.options.providers[this.provider.id];
    let taskOptions = this.options.tasks[opts.key];
    if (taskOptions) {
      //If there are spicific queue size config for task (can be < or > current), override
      queueSizeConcurrent = taskOptions.queueSizeConcurrent;
      parserQueueSizeConcurrent = taskOptions.parserQueueSizeConcurrent;
    }
    if (providerOptions) {
      //If provider's queue size config < current, apply this limit
      if (providerOptions.queueSizeConcurrent < queueSizeConcurrent)
        queueSizeConcurrent = providerOptions.queueSizeConcurrent;
      if (providerOptions.parserQueueSizeConcurrent < parserQueueSizeConcurrent)
        parserQueueSizeConcurrent = providerOptions.parserQueueSizeConcurrent;
    }

    //Start here:
    let cntLost;
    this.provider.setQueueConcurrentSize(queueSizeConcurrent);
    return this.provider.setParserQueueConcurrentSize({
      queueConcurrentSize: parserQueueSizeConcurrent
    }).then(() => startGrab())
      .then((started) => {
        if (!started)
          return {
            skipped: true
          };
        else
          return grabLostOrNext()
          .then((pickedIdsCnt) => new Promise((resolve, reject) => {
          this.eventEmitter.once(grabStopEventKey, () => {
            stopIter().then((res) => {
              resolve(res);
            }).catch((err) => {
              reject(err);
            });
          });

          this.eventEmitter.once(grabCompleteEventKey, () => {
            completeIter().then((res) => {
              resolve(res);
            }).catch((err) => {
              reject(err);
            });
          });

          checkIfShouldStop();
        }))
        .then((res) => {
          res.isComplete = (!res.stopped && !res.cntRetryIds);
          if (!res.isComplete)
            res.retry = true;
          if (res.cntSuccessIds == 0 && res.cntNotFoundIds == 0 
            && res.errs.length > 0 && res.stopped)
            res.isBad = true; //no useful result, only errors
          this.dateStart = null;
          this.eventEmitter.removeAllListeners(grabCompleteEventKey);
          this.eventEmitter.removeAllListeners(grabStopEventKey);
          return res;
        }).catch((err) => {
          this.eventEmitter.removeAllListeners(grabCompleteEventKey);
          this.eventEmitter.removeAllListeners(grabStopEventKey);
          throw err;
        });
      });
  }
  
  /**
   * speed in ids/sec
   */
  getGrabSpeed() {
    return this.dateStart !== null ? 
      (this.cntUsefulProcessedIds / ((new Date() - this.dateStart) / 1000)).toFixed(2)
      : null;
  }

  /**
   *
   */
  isLastIterDoneOk(key) {
    let grabIterKey = 'mal.' + key + '.grabIter';
    let retryIdsKey = 'mal.' + key + '.retryIds';
    let dateIterStartKey = 'mal.' + key + '.dateIterStart';
    let dateIterEndKey = 'mal.' + key + '.dateIterEnd';

    return this.redis.mgetAsync([grabIterKey, dateIterEndKey, retryIdsKey])
    .then(([grabIter, dateIterEnd, retryIds]) => {
      return grabIter !== null && retryIds === null && dateIterEnd !== null;
    });
  }


}
var cls = MalBaseScanner; //for using "cls.A" as like "self::A" inside class

module.exports = MalBaseScanner;
