/**
 * Explicit matrix factorization
 * Algos: ALS (primary, parallel), SGD (1-thread only)
 *
 * Inspired by:
 * (rus) https://habrahabr.ru/company/yandex/blog/241455/
 * (!) http://www.grappa.univ-lille3.fr/~mary/cours/stats/centrale/reco/paper/MatrixFactorizationALS.pdf
 * (has err in als) http://blog.ethanrosenthal.com/2016/01/09/explicit-matrix-factorization-sgd-als/
 *
 * Also:
 * (implicit mf) http://yifanhu.net/PUB/cf.pdf
 * (kNN) http://blog.ethanrosenthal.com/2015/11/02/intro-to-collaborative-filtering/
 *
 * Similar products:
 * https://mahout.apache.org/users/recommender/intro-als-hadoop.html
 * http://spark.apache.org/docs/latest/ml-collaborative-filtering.html
 * http://predictionio.incubator.apache.org/templates/recommendation/quickstart/
 * http://docs.seldon.io/ml100k.html
 *
 * @author ukrbublik
 */

/*
todo: 
1. optionally allow to recommend from not EmfRecommender, 
 and recommend while training 
1. is_used_for_train
2. calc rmse separately!!!
3. get part of users (not all!) for quick recommend/rmse
4. use anime-2-anime recs
5. npm quick-tcp-socket
6. emf singleton
...
profit
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
