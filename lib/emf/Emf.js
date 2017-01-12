/**
 * Explicit matrix factorization
 * Algos: ALS (primary, parallel), SGD (1-thread only)
 *
 * Inspired by:
 * http://www.grappa.univ-lille3.fr/~mary/cours/stats/centrale/reco/paper/MatrixFactorizationALS.pdf
 * https://habrahabr.ru/company/yandex/blog/241455/
 * http://blog.ethanrosenthal.com/
 *
 * See also (other algos, implicit matrix factorization):
 * http://yifanhu.net/PUB/cf.pdf
 *
 * Similar products:
 * https://mahout.apache.org/users/recommender/intro-als-hadoop.html
 * http://spark.apache.org/docs/latest/ml-collaborative-filtering.html
 * http://predictionio.incubator.apache.org/templates/recommendation/quickstart/
 * http://docs.seldon.io/ml100k.html
 *
 * @author ukrbublik
 */

const EmfBase = require('./EmfBase');
const EmfManager = require('./EmfManager');
const EmfRecommender = require('./EmfRecommender');
const EmfMaster = require('./EmfMaster');
const EmfWorker = require('./EmfWorker');
const EmfLord = require('./EmfLord');
const EmfChief = require('./EmfChief');

class EmfFactory {
  /**
   *
   */
  static createRecommender() {
    return new EmfRecommender();
  }

  /**
   *
   */
  static createLord() {
    return new EmfLord();
  }

  /**
   *
   */
  static createChief() {
    return new EmfChief();
  }
  
  /**
   *
   */
  static createWorker(isClusterMaster, workerId, workerProcess) {
    return new EmfWorker(isClusterMaster, workerId, workerProcess);
  }

}

module.exports = {
  EmfFactory: EmfFactory
};
