/**
 * MAL task queue
 * For now simple task queue with 1 processing task at time.
 *
 * @author ukrbublik
 */


const deepmerge = require('deepmerge');
const assert = require('assert');
const _ = require('underscore')._;
const ProgressBar = require('progress');
const MalError = require('./MalError');
const MalScanner = require('./MalScanner');
const MalApiClient = require('./MalApiClient');
const MalParser = require('./MalParser');
const Helpers = require('../Helpers');
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
const EventEmitter = require('events');


/**
 * 
 */
class MalTaskQueue {
  constructor() {
    this.allScanners = {};
    this.runningTasks = {};
  }

  /**
   *
   */
  init(config) {
    this.options = config;

    this.redisClientId = this.genUniqRedisClientId();

    this.db = pgp(this.options.db);
    this.redis = redis.createClient(deepmerge.all([this.options.redis, {
      detect_buffers: true,
    }]));
    this.redis.on("error", (err) => {
      console.error("Redis error: ", err);
    });
    return new Promise((resolve, reject) => {
      //wait for connection to redis
      this.redis.once("error", (err) => {
        reject(err);
      });
      this.redis.once("connect", () => {
        resolve();
      });
    })
    .then(() => this.redis.send_commandAsync("client", ["setname", this.redisClientId]))
    .then(() => {
      //Create providers
      let prvInitPromises = [];
      let allProviders = {};
      for (let id in config.providers) {
        let providerOptions = deepmerge.all([ config.provider, config.providers[id] ]);
        let provider = providerOptions.type == 'apiClient' ? new MalApiClient() : new MalParser();
        allProviders[id] = provider;
        prvInitPromises.push(provider.init(providerOptions, id).catch((err) => {
          console.log("Failed to init provider " + id + ": ", err);
          delete allProviders[id];
        }));
      }
      return Promise.all(prvInitPromises).then(() => {
        //Create scanners (1 scanner per 1 provider)
        let scnInitPromises = [];
        for (let id in allProviders) {
          let provider = allProviders[id];
          let scanner = new MalScanner();
          this.allScanners[id] = scanner;
          scanner.setProvider(id, provider);
          scnInitPromises.push(scanner.init(config));
        }
        return Promise.all(scnInitPromises)
        .then(() => this.redis.getAsync("mal.processingTask"))
        .then((lastPocessingTask) => {
          if (lastPocessingTask) {
            //If last execution failed while processing some task, prepend it to queue
            return this.redis.multi()
              .del("mal.processingTask")
              .lpush("mal.queuedTasks", lastPocessingTask)
              .execAsync();
          }
        }).then(() => {
          console.log("MalTaskQueue inited");
        });
      })
    });
  }

  /**
   *
   */
  genUniqRedisClientId() {
    function s4() {
      return Math.floor((1 + Math.random()) * 0x10000)
        .toString(16)
        .substring(1);
    }
    return 'mal.client_' + new Date().getTime() + '_' + s4() + s4() + s4();
  }

  /**
   *
   */
  isQueueEmpty() {
    return this.redis.llenAsync("mal.queuedTasks")
      .then((queuedTasksLen) => (!queuedTasksLen));
  }

  /**
   *
   */
  isProcessingTask()  {
    return Object.keys(this.runningTasks) > 0;
  }

  /**
   *
   */
  addTasksToQueue(tasksKeys, onlyIfQueueIsEmpty = true) {
    return (!onlyIfQueueIsEmpty ? Promise.resolve() : this.isQueueEmpty())
      .then((isQueueEmpty) => {
        if (!onlyIfQueueIsEmpty || isQueueEmpty) {
          for (let taskKey of tasksKeys) {
            this.redis.rpush("mal.queuedTasks", taskKey);
          }
          return true;
        } else return false;
      });
  }

  /**
   * For now only 1 processing task at time
   */
  runTaskQueueLoop() {
    let retryTimeWhenNoFreeScanners = 5*1000; //5s
    let timeToPopNextTask = 1*1000; //1s

    let isNoMoreMoreTasks = false;
    return Helpers.promiseWhile(() => !isNoMoreMoreTasks, () => {
      let freeScannersIds = Object.keys(this.allScanners)
        .filter((id) => (!this.allScanners[id].isBusy && !this.allScanners[id].isDead));
      if (freeScannersIds.length == 0) {
        console.log("> No free scanners, retry in " + retryTimeWhenNoFreeScanners + "ms");
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            resolve();
          }, retryTimeWhenNoFreeScanners);
        });
      }
      return this.redis.lpopAsync("mal.queuedTasks").then((nextTask) => {
        if (!nextTask)
          isNoMoreMoreTasks = true;
        else {
          return this.redis.setAsync("mal.processingTask", nextTask)
          .then(() => this.performTask(nextTask))
          .catch((err) => {
            return false;
          })
          .then((successed) => {
            let m = this.redis.multi();
            m.del("mal.processingTask");
            if (!successed)
              m.rpush("mal.queuedTasks", nextTask);
            return m.execAsync(); //should be safe
          });
        }
      });
    }).then(() => new Promise((resolve, reject) => {
      setTimeout(() => {
        return this.runTaskQueueLoop();
      }, timeToPopNextTask);
    }));
  }

  /**
   * Promise result - bool, true if task processed successfully (or skipped), false if failed 
   *  (because of no free scanners)
   */
  performTask(taskKey) {
    let retryTaskTimeout = this.options.taskQueue.retryTaskTimeout; //10s

    return MalScanner.shouldSkipTask(taskKey).then((shouldSkip) => {
      if (shouldSkip) {
        console.log('> Skipping task ' + taskKey);
        return Promise.resolve(true);
      } else {
        return Promise.resolve().then(() => {
          let maxProvidersCnt = Object.keys(this.allScanners).length;
          let freeScannersIds, activeScannersIds;

          //Check that there are free scanners to start, 
          // get first to init task (see MalScanner.beforeTask)
          freeScannersIds = Object.keys(this.allScanners)
            .filter((id) => !this.allScanners[id].isBusy && !this.allScanners[id].isDead);
          if (freeScannersIds.length == 0) {
            console.log('> No free scanners for task ' + taskKey + '!');
            return Promise.resolve(false);
          }
          let anyScanner = this.allScanners[freeScannersIds[0]];
          anyScanner.isBusy = true;
          return MalScanner.beforeTask(taskKey, anyScanner).then((data) => {
            anyScanner.isBusy = false;

            //Start task
            let taskEE = new EventEmitter();
            activeScannersIds = [];
            this.runningTasks[taskKey] = {
              activeScannersIds: activeScannersIds,
              ee: taskEE,
              data: data,
            };
            //If task won't succeed at once because of errors, retry until success
            let isTaskSuccessed = false;
            let isTaskFailed = false;
            let tryCount = 0;
            return Helpers.promiseWhile(() => !(isTaskSuccessed || isTaskFailed), () => {
              let logTimer;
              return new Promise((resolve, reject) => {
                tryCount++;
                console.log('> ' + (tryCount == 1 ? 'Starting' : 'Retrying') + ' task ' + taskKey);
                let i = 0;
                logTimer = setInterval(() => {
                  //every 30s log speed for all providers
                  this.logGrabSpeed(taskKey, (i % (this.options.isTest ? 10 : 30) == 0));
                  i++;
                }, 1*1000); //log every 1s

                //Pick free scanners
                freeScannersIds = Object.keys(this.allScanners).filter((id) => 
                  (!this.allScanners[id].isBusy && !this.allScanners[id].isDead));
                if (freeScannersIds.length == 0) {
                  console.log('> No free scanners for task ' + taskKey + '!');
                  isTaskFailed = true;
                  resolve();
                }
                assert( activeScannersIds.length == 0);
                activeScannersIds.push(...freeScannersIds.splice(0, maxProvidersCnt));

                //First start one scanner, after 1 sec start others.
                //It's not necessary, but when grabbing by list of ids it's better to let
                // first scanner set sharedListsOfIds and others get it without fetching redis.
                let firstScannerId = activeScannersIds[0];
                this.addScannerToTask(firstScannerId, taskKey);
                setTimeout(() => {
                  if (this.runningTasks[taskKey] !== undefined) {
                    let otherScannersIds = activeScannersIds.filter((id) => (id != firstScannerId));
                    for (let id of otherScannersIds) {
                      this.addScannerToTask(id, taskKey);
                    }
                  }
                }, 1*1000); //1s

                //Watch result & err events from scanners
                let doneScannerIds = [];
                taskEE.on("result", (id, res) => {
                  activeScannersIds.splice(activeScannersIds.indexOf(id), 1);
                  //Stopped (because of errors) scanners will be released (isBusy = false) right
                  // away, but others (doneScannerIds) will be released at once after all done
                  // (for future, if there will be > 1 tasks running at once)
                  if (!res.stopped)
                    doneScannerIds.push(id);
                  if (res.isComplete)
                    isTaskSuccessed = true;
                  if (activeScannersIds.length == 0) {
                    console.log("> Result: ", res);
                    if (isTaskSuccessed) {
                      //all good!
                      for (let _id of doneScannerIds) {
                        let _scanner = this.allScanners[_id];
                        _scanner.isBusy = false;
                      }
                      clearInterval(logTimer);
                      console.log('> Completed task ' + taskKey);
                      resolve();
                    } else {
                      //retry
                      console.log('> Retrying task ' + taskKey 
                        + ' in ' + retryTaskTimeout + 'ms..');
                      clearInterval(logTimer);
                      setTimeout(() => {
                        for (let _id of doneScannerIds) {
                          let _scanner = this.allScanners[_id];
                          _scanner.isBusy = false;
                        }
                        resolve();
                      }, retryTaskTimeout);
                    }
                  }
                });
                taskEE.on("err", (id, err) => {
                  console.error('> Error! [' + id + ']', err);
                  reject(err);
                });
              }).then(() => {
                taskEE.removeAllListeners();
              }).catch((err) => {
                taskEE.removeAllListeners();
                clearInterval(logTimer);
                throw err;
              });
            }).then(() => {
              delete this.runningTasks[taskKey];
              return MalScanner.afterTask(taskKey, isTaskSuccessed).then(() => {
                return isTaskSuccessed;
              });
            }).catch((err) => {
              delete this.runningTasks[taskKey];
              return MalScanner.afterTask(taskKey, false).then(() => {
                throw err;
              });
            });
          });
        });
      }
    });
  }

  /**
   * 
   */
  addScannerToTask(id, taskKey) {
    let badProviderCooldownTime = this.options.taskQueue.badProviderCooldownTime; //60s
    let badResultsToMarkAsDead = this.options.taskQueue.badResultsToMarkAsDead; //5

    let scanner = this.allScanners[id];
    let task = this.runningTasks[taskKey];
    if (this.runningTasks[taskKey] !== undefined && !scanner.isBusy) {
      scanner.isBusy = true;
      scanner.badResults = 0;
      if (task.activeScannersIds.indexOf(id) == -1)
        task.activeScannersIds.push(id);
      scanner.doGrabTask(taskKey, task.data).then((res) => {
        //console.log("[" + id + "] " + "* Result: ", res);
        console.log("[" + id + "] " + "* " 
          + (res.stopped ? "Stopped" : (res.skipped ? "Skipped" : "Done")) 
          + (res.errs && res.errs.length ? " with " + res.errs.length + " errors" : ""));
        if (res.stopped) {
          if (res.isBad)
            scanner.badResults++;
          else
            scanner.badResults = 0;
          if (scanner.badResults == badResultsToMarkAsDead) {
            //Dead provider!
            console.log("*** Dead provider " + id);
            scanner.isBusy = false;
            scanner.isDead = true;
            delete this.allScanners[id];
          } else {
            //Retry it after cooldown
            console.log("*** Stopped provider " + id + ", cooldown for " 
              + badProviderCooldownTime + "ms");
            setTimeout(() => {
              scanner.isBusy = false;
              //(For future) Here we can add scanner to another running task
              this.addScannerToTask(id, taskKey);
            }, badProviderCooldownTime);
          }
        } else {
          scanner.badResults = 0;
        }
        task.ee.emit("result", id, res);
      }).catch((err) => {
        scanner.isBusy = false;
        task.ee.emit("err", id, err);
      });
    }
  }

  /**
   * 
   */
  logGrabSpeed(taskKey, ext = false) {
    let speeds = {}, sum = 0;
    let taskInfo = this.runningTasks[taskKey];
    for (let id of taskInfo.activeScannersIds) {
      let scanner = this.allScanners[id];
      let speed = scanner.getGrabSpeed();
      speeds[scanner.id] = speed;
      if (!isNaN(parseFloat(speed)))
        sum += parseFloat(speed);
    }

    if (ext) {
      let cntSuccessIdsKey = 'mal.' + taskKey + '.cntSuccessIds';
      let cntNotFoundIdsKey = 'mal.' + taskKey + '.cntNotFoundIds';
      let cntIdsKey = 'mal.' + taskKey + '.cntIds';
      this.redis.mgetAsync([cntSuccessIdsKey, cntNotFoundIdsKey, cntIdsKey])
      .then(([rCntSuccessIds, rCntNotFoundIds, rCntIds]) => {
        if (rCntSuccessIds !== null)   rCntSuccessIds = parseInt(rCntSuccessIds);
        if (rCntNotFoundIds !== null)  rCntNotFoundIds = parseInt(rCntNotFoundIds);
        if (rCntIds !== null)          rCntIds = parseInt(rCntIds);
        let cntRestIds = rCntIds - (rCntSuccessIds + rCntNotFoundIds);
        let estSecs = rCntIds && sum ? (cntRestIds / sum) : 0;
        console.log("Total speed: " + sum.toFixed(2) + ". " 
          + (estSecs ? "Estimated: " + (estSecs/60).toFixed(1) + " mins. " : "")
         + "By scanners: " + JSON.stringify(speeds)
        );
      });
    } else
      console.log("Total speed: " + sum.toFixed(2));
  }


}
var cls = MalTaskQueue; //for using "cls.A" as like "self::A" inside class

module.exports = MalTaskQueue;