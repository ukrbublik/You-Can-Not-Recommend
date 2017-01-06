/**
 * Simple class to wrap process messages as events
 *
 * @author ukrbublik
 */

const EventEmitter = require('events');

class EmfProcess extends EventEmitter {
	constructor(options) {
		super();

		if (options === undefined)
			options = {};
		if (options.state === undefined)
			options.state = -1;
		if (options.process === undefined)
			options.process = process;

		this.process = options.process;
		//if true - current process is worker itself, see in EmfWorkerProcess.js
		this.isCurrentProcess = (this.process === process);
		//-1 - initing, 0 - ready, 1 - busy
		this.state = options.state;

		if (!this.isCurrentProcess) {
			this.id = options.id;
			this.portionBuffer = options.portionBuffer;
			this.useForTrain = options.useForTrain;
		}

		this.process.on('message', this.onMessage.bind(this));
		this.process.on('beforeExit', this.onBeforeExit.bind(this));
    this.process.on('exit', this.onExit.bind(this));
		this.process.on('error', this.onError.bind(this));
	}

	emit(eventName, ...args) {
		let msg = eventName, data;
		if (args.length >= 1)
			data = args[0];
		if (data === undefined)
			data = {};
		let packet = {
			msg: msg,
			data: data
		};
    if (this.process) {
		  this.process.send(packet);
    }
	}

	_emit(eventName, ...args) {
		super.emit(eventName, ...args);
	}

	onMessage(packet) {
		if (packet.msg == 'inited') {
			this.state = 0;
		}
		this._emit(packet.msg, packet.data);
	}

	onExit(code, signal) {
    this.process = null;
		this._emit('exit', code, signal);
	}

  onBeforeExit(code) {
    this.process = null;
  }

	onError(err) {
    if (this.process) {
		  this._emit('error', err);
    }
	}

}

module.exports = EmfProcess;
