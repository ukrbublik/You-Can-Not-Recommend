/**
 * Don't use this directly!
 * It's child worker process spawned from EmfMaster.createWorkers()
 *
 * @author ukrbublik
 */

const EmfFactory = require('./Emf').EmfFactory;
const EmfProcess = require('./EmfProcess');

var emfProcess = new EmfProcess();
var emf = null;
var alive = true;

setInterval(() => {
  if (!alive)
    process.exit(0);
}, 1000);

emfProcess.once('create', (data) => {
  //console.log('[Worker#'+data.workerId+']', 'Initing');
  emf = EmfFactory.createWorker(data.isClusterMaster, data.workerId, emfProcess);
  emf.init(data.config, data.options).then(() => {
    //console.log('[Worker#'+emf.workerId+']', 'Inited');
    emf.process.emit('created');
  });
});


process.on('unhandledRejection', function (err) {
  console.error('[Worker#'+(emf ? emf.workerId : '?')+']', err);
  emfProcess._emit('kill');
});
process.on('uncaughtException', function (err) {
  console.error('[Worker#'+(emf ? emf.workerId : '?')+']', err);
  emfProcess._emit('kill');
});

emfProcess.on('kill', () => {
  if (emf) {
    emf.destroy();
    emf = null;
  }
  alive = false;
});
