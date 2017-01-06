/**
 * Some helper functions
 *
 * @author ukrbublik
 */

const fs = require('fs');
const assert = require('assert');
const _ = require('underscore')._;

class Helpers {

  static promiseWhile (condition, action) {
    if (!condition())
      return Promise.resolve();
    return Promise.resolve()
      .then(action)
      .then((res) => condition() ? cls.promiseWhile(condition, action) : res);
  };

  static promiseWait (condition, interval, action) {
    if (condition())
      return Promise.resolve().then(action);
    return new Promise((resolve, reject) => {
      let timer;
      let runTimer = () => {
        timer = setTimeout(() => {
          if (condition()) {
            Promise.resolve().then(action).then((res) => {
              resolve(res);
            }).catch((err) => {
              reject(err);
            });
          } else {
            clearTimeout(timer);
            runTimer();
          }
        }, interval);
      };
      runTimer();
    });
  };

  static promiseDoWhile (condition, action) {
    return Promise.resolve()
      .then(action)
      .then((res) => condition() ? cls.promiseDoWhile(condition, action) : res);
  };

  //from min to max (including max)
  static getRandomInt(min, max) {
    return Math.floor(Math.random() * (max + 1 - min)) + min;
  }

  static isSameElementsInArrays(arr1, arr2) {
    return _.difference(arr1, arr2) == 0 && _.difference(arr2, arr1) == 0;
  }

  static humanFileSize(size) {
    let i = Math.floor( Math.log(size) / Math.log(1024) );
    if (i <= 0)
      return "0";
    else
      return ( size / Math.pow(1024, i) ).toFixed(2) * 1 + ' ' + ['B', 'kB', 'MB', 'GB', 'TB'][i];
  };

  static pipePromise(readStream, writeStream) {
    return new Promise((resolve, reject) => {
      readStream.pipe(writeStream);
      writeStream.once('finish', () => {
        readStream.unpipe(writeStream);
        resolve();
      });
      readStream.once('error', (err) => {
        reject(err);
      });
      writeStream.once('error', (err) => {
        reject(err);
      });
    });
  }

  static copyFilePromise(fromFile, toFile) {
    let readStream = fs.createReadStream(fromFile);
    let writeStream = fs.createWriteStream(toFile);
    return cls.pipePromise(readStream, writeStream);
  }

  //Create empty file with specific size
  static createFile(path, size = 0, recreate = false) {
    let bufSize = 1024*1024*10; //10MB
    let stat = fs.existsSync(path) ? fs.statSync(path) : null;
    if (!stat || !stat.isFile()) {
      fs.writeFileSync(path, '');
    }
    if (size) {
      let buf, s, b;
      if (recreate || !stat || stat.size > size) {
        //fill with zeros
        fs.writeFileSync(path, '');
        buf = Buffer.alloc(bufSize);
        for (let i = 0 ; i < size ; i += bufSize) {
          s = Math.min(bufSize, size - i*bufSize);
          b = buf.slice(0, s);
          fs.appendFileSync(path, b);
        }
      } else if (stat.size < size) {
        //append zeros
        buf = Buffer.alloc(bufSize);
        let diff = size - stat.size;
        for (let offs = 0 ; offs < diff ; offs += bufSize) {
          s = Math.min(bufSize, diff - offs);
          b = buf.slice(0, s);
          fs.appendFileSync(path, b);
        }
      } //else size match
    }
  }

}
var cls = Helpers; //for using "cls.A" as like "self::A" inside class

module.exports = Helpers;
