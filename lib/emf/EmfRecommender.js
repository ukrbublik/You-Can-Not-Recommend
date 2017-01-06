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
  init(config, options = {}) {
    options = Object.assign({}, options, {
      keepFactorsOpened: true,
      //todo_later: факторы item-ов можно держать в памяти (не shm), т.к. их в mal не очень много
      lowmem: true, //read factors from files
    });
    return super.init(config, options);
  }

  /**
   *
   */
  initCluster() {
    return Promise.resolve();
  }

  /**
   * Theoretically, trainers (descendants of EmfMaster) can also recommend while NOT trainig,
   *  but by design only EmfRecommender is used to recommend
   */
  recommendItemsForUser(userId, limit = 20) {
    //todo: 
    // 1. исключать тайтлы в списке MAL
    // 2. учитывать рекомендации явные (от тайтла к тайтлу)
    // 3. не показывать 2+ тайтла 1 франшизы, только минимальную часть (и не ova/ona, если возможно)
    //   в бд сделать таблицу "франшизы", с полем "main_id"
    // 4. потом более умные соц. рекомендации - смотреть на любимые тайтлы друзей, соклубников.
    // сделать класс socialrecommender
    
    // 5. попробовать kNN - ближайшие соседи по любимым тайтлам - 
    //  10 fav (сохранять сортировнно по id?)  + те, которым поставил 10,9 (или 8, если это макс оценка юзера) (эти доп.любимые хранить в бд и высчитывать после обновления листа)
    //  при этом друзья и соклубники имеют больший приоритет (чем больше клубов, тем больше пр.)
    // И/ИЛИ тупо вычислять расхождение общих оценок - долго и нужно для каждого юзера отдельно, но можно распараллелить
    // knn - сделать класс knn_recommender
    //
    // в бд для юзера вынести тпблицу "user_lists", где будут avg_rating, cnt_ratings, @array fav10 и @array toprated
    

    return new Promise((resolve, reject) => {
      if (this._status != 'ready') {
        reject("Status is not ready");
      } else if (!this.areSharedFactorsOpened()) {
        reject("Factors not loaded");
      } else {
        this.db.one("\
          select array_to_string(array_agg(item_id), ',') as item_ids \
          from malrec_ratings \
          where user_list_id = $(userId) \
          group by user_list_id \
        ", {
          userId: userId,
        }).then((data) => {
          let recItems = [];
          let minRatingInSelection = 0;
          let userFactors = this.getFactorsRowSync('byUser', userId);
          let ratedItemIds = data.item_ids.split(',');
          let ratedItemIdsA = {};
          for (let itemId of ratedItemIds)
            ratedItemIdsA[itemId] = 1;
          let predict;
          let minRecommendRating = this.minRecommendRating; //todo: take to account user's avg rating
          for (let itemId = 0 ; itemId < this.itemsCount ; itemId++) {
            if (!ratedItemIdsA[itemId]) {
              predict = this.predictSync(userId, itemId, userFactors);
              if (predict >= minRecommendRating 
                && (recItems.length < limit || predict > minRatingInSelection)) {
                recItems.push({predict: predict, id: itemId});
                recItems.sort((a, b) => b.predict - a.predict);
                if (predict > minRatingInSelection)
                  minRatingInSelection = predict;
                if (recItems.length >= limit)
                  recItems.pop();
              }
            }
          }
          resolve(recItems);
        });
      }
    });
  }

}
var cls = EmfRecommender; //for using "cls.A" as like "self::A" inside class

module.exports = EmfRecommender;
