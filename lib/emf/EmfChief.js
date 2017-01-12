/**
 * Master process, slave node in cluster
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



/**
 * 
 */
class EmfChief extends EmfMaster {
  
  /**
   *
   */
  constructor() {
    super(false);
  }

  /**
   *
   */
  prepareToTrain() {
    throw new Exception("No manual call. Only by event 'prepareToTrain' from Lord.");
  }

  /**
   *
   */
  getStats() {
    return new Promise((resolve, reject) => {
      this.lordClient.sendPacket("getStats", null, () => {
        this.lordClient.once("setStats", (data) => {
          this.stats = data.stats;
          this.options = Object.assign(this.options, data.options);
          resolve();
        });
      });
    });
  }

  /**
   *
   */
  loadFactorsFromLord() {
    return new Promise((resolve, reject) => {
      let t1 = Date.now();
      let siz = this.getFactorsByteLength();
      this.lordClient.sendPacket("getFactors", null, () => {
        this.lordClient.once("setFactors", () => {
          this.lordClient.receiveStreams(this.getFactorsWriteStreams(), () => {
            let t2 = Date.now();
            let sp = (siz) / ((t2-t1) / 1000); //B/s
            console.log("U/I factors (" + Helpers.humanFileSize(siz) + ") transferred in " 
              + (t2-t1) + "ms (" +  Helpers.humanFileSize(sp) + "/s)");
            resolve();
          });
        });
      });
    });
  }
  
  /**
   *
   */
  initCluster() {
    if (this.options.useClustering) {
      return this.initClusterChief();
    } else {
      return Promise.resolve();
    }
  }

  /**
   *
   */
  initClusterChief() {
    return new Promise((resolve, reject) => {
      this.clusterNodes = {};
      this.myClients = {};

      // 1. Client socket connected to Lord
      let lordClientSocket = net.connect(this.options.clusterMasterPort, 
        this.options.clusterMasterHost, () => {
        console.log("Connected to Lord");
        
        this.clusterNodes['m'] = this.lordClient;

        this.lordClient.sendPacket("register", {
          port: this.options.clusterServerPort,
          host: os.hostname(),
        }, () => {
          this.lordClient.once("registered", (data) => {
            this.lordClient.data.nodeId = data.nodeId;
            console.log("Lord registered me as #" + data.nodeId);
            resolve();
          });
        });

        this.lordClient.on("meetCluster", (data) => {
          let meetCnt = Object.keys(data.nodes).length;
          let metCnt = 0;
          let checkDone = () => {
            if (metCnt == meetCnt) {
              this.lordClient.sendPacket("metCluster", null);
            }
          };

          for (let nodeId in data.nodes) {
            if (this.clusterNodes[nodeId]) {
              //already connected
              metCnt++;
            } else {
              let node = data.nodes[nodeId];
              let nodeClient = null;
              let nodeClientSocket = net.connect(node.port, node.host, () => {
                nodeClient = new TcpSocket(false, nodeClientSocket);

                nodeClient.sendPacket("setMyNodeId", {
                  "nodeId": this.lordClient.data.nodeId
                }, () => {
                  nodeClient.once("gotYourNodeId", () => {
                    console.log("Connected to cluster node #" + nodeId);
                    this.clusterNodes[nodeId] = nodeClient;
                    metCnt++;
                    checkDone();
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

          checkDone();
        });
      });
      this.lordClient = new TcpSocket(false, lordClientSocket);

      this.lordClient.on('close', () => {
        let wasConnected = (this.clusterNodes['m'] == this.lordClient);
        console.log(wasConnected ? 'Disconnected from Lord' : 'Connection to Lord failed');
        this.disconnectFromCluster();
        this._status = "";
        this.destroy();
      });

      this.lordClient.on('error', (err) => {
        console.log("Lord socker error: ", err);
      });


      // 2. Server socket, all clients should be connected to it
      this.chiefServer = net.createServer({}, (clientSocket) => {
        let client = new TcpSocket(true, clientSocket);
        let nodeId = null;

        client.once("setMyNodeId", (data) => {
          client.data.nodeId = data.nodeId;
          nodeId = data.nodeId;
          this.myClients[nodeId] = client;
          client.sendPacket("gotYourNodeId", null);
          console.log("Cluster node #" + nodeId + " client connected");

          if (nodeId == 'm') {
            client.on("prepareToTrain", (data) => {
              this.cl_prepareToTrain(client, data);
            });
            client.on("startTrain", () => {
              this.cl_startTrain(client);
            });
            client.on("endTrain", (info) => {
              this.cl_endTrain(client, info);
            });
            client.on("startAlsTrainStep", (data) => {
              this.cl_startAlsTrainStep(client, data);
            });
            client.on("alsTrainStepEnd", () => {
              this.cl_alsTrainStepEnd(client);
            });
            client.on("startCalcRmse", (data) => {
              this.cl_startCalcRmse(client, data);
            });
            client.on("endCalcRmse", (data) => {
              this.cl_endCalcRmse(client, data);
            });
          }
        });

        client.on("alsSaveCalcedFactors", (data) => {
          let stream = this.getWriteStreamToSaveCalcedFactors(this.stepType, data.rowsRange);
          client.receiveStreams([stream], () => {
            this.m_completedPortion(data, client.data.nodeId);
          });
        });

        client.on('close', () => {
          if (nodeId && this.myClients[nodeId]) {
            console.log("Cluster node #" + nodeId + " disconnected");
            delete this.myClients[nodeId];
          }
        });

        client.on('error', (err) => {
          console.log("Cluster node #" + nodeId + " client socker error: ", err);
        });

      });
      this.chiefServer.listen(this.options.clusterServerPort, () => {
        console.log("Listening cluster on port " + this.options.clusterServerPort);
      });

    });
  }

  /**
   * data.lastCalcDate
   */
  cl_prepareToTrain(client, data) {
    let needToSync = data.lastCalcDate === null || data.lastCalcDate != this.lastCalcDate;
    this._status = "preparing";
    console.log('Preparing to train...');
    this.getStats().then(() => {
      return this.prepareSharedFactors();
    }).then(() => {
      return this.prepareWorkersToTrain();
    }).then(() => {
      return promiseFactors = needToSync ? this.loadFactorsFromLord() : Promise.resolve();
    }).then(() => {
      this.lordClient.sendPacket("preparedToTrain", null, () => {
        console.log("Total shared memory used: " + Helpers.humanFileSize(shm.getTotalSize()));
        console.log('Prepared to train');
      });
    });
  }

  /**
   * 
   */
  cl_startTrain (client) {
    this._status = "training";
    console.log('*** Starting train...');
  }

  /**
   * calcInfo - see getCalcInfo()
   */
  cl_endTrain (client, calcInfo) {
    this.detachWorkPortionBuffers();
    this.saveCalcResults(calcInfo).then(() => {
      this.broadcastMessageToWorkers("endTrain");
      client.sendPacket("endedTrain");
      this._status = "ready";
      console.log('Peak memory usage: ' + this.formatPeakMemoryUsage());
      console.log('*** Training complete');
    });
  }

  /**
   * data.stepType
   */
  cl_startAlsTrainStep(client, data) {
    this.trainIter = data.trainIter;
    this._startAlsTrainStep(data.stepType);
  }

  /**
   *
   */
  cl_alsTrainStepEnd(client) {
  }

  /**
   * data.stepType
   * data.useGlobalAvgShift
   */
  cl_startCalcRmse(client, data) {
    this._startCalcRmse(data.stepType, data.useGlobalAvgShift);
  }

  /**
   * data.globalAvgShift
   */
  cl_endCalcRmse(client, data) {
    this._endCalcRmse(data.globalAvgShift);
  }

  /**
   * 
   */
  _incrNextPortion(wantPortions, callback) {
    let reqId = this.reqId++;
    this.lordClient.sendPacket("getNextPortions", {
      wantPortions: wantPortions,
      reqId: reqId,
    }, () => {
      this.lordClient.once("retNextPortions_"+reqId, (data) => {
        callback(data.portions);
      });
    });
  }

}
var cls = EmfChief; //for using "cls.A" as like "self::A" inside class

module.exports = EmfChief;
