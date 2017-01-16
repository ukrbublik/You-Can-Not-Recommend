/**
 * Main controller
 *
 * @author ukrbublik
 */

const deepmerge = require('deepmerge');
const extractZip = require('extract-zip');
const _ = require('underscore')._;
const http = require('http');
const fs = require('fs');
const assert = require('assert');
const pgp = require('pg-promise')({
  error: (err, e) => {
    console.error("PGSQL error: ", err, 
      "\n Query: ", e.query, 
      "\n Params: ", e.params,
      "\n Ctx: ", e.ctx
    );
  }
});
const Helpers = require('./Helpers');
const EmfFactory = require('./emf/Emf').EmfFactory;


/**
 * 
 */
class YcnrController {

  /**
   * @return array default options
   */
  static get DefaultOptions() {
    return {
      db: null,
      dbType: 'ml', //'ml', 'mal'
      maxRating: {
        mal: 10,
        ml: 5,
      },
      minRecommendRating: {
        mal: 7, //of 10
        ml: 3.5, //of 5
      },
    }
  }

  //sugar getter, setters:
  get minRecommendRating() { 
    if (this.options.minRecommendRating[this.options.dbType])
      return this.options.minRecommendRating[this.options.dbType];
    else
      return this.options.maxRating[this.options.dbType] * 0.7;
  }
  get maxRating() { return this.options.maxRating[this.options.dbType] }

  /**
   *
   */
  constructor() {
  }

  /**
   *
   */
  init(isMaster, config, options = {}) {
    this.emf = isMaster ? EmfFactory.createLord() : EmfFactory.createChief();

    this.options = deepmerge.all([cls.DefaultOptions, config.common, {
      db: config.db,
      redis: config.redis,
    }, options]);

    this.db = pgp(this.options.db[this.options.dbType]);

    return Promise.all([
      this.emf.init(config, {}, this.db),
      this.loadItems(),
    ]).then(() => {
      setInterval(() => {
        this.emf.fixPeakMemoryUsage();
      }, 1000);

      this.emf.on("destroyed", () => {
        process.exit(0);
      });
    });
  }

  /**
   * @param string size '1m' or '100k'
   */
  importML(size) {
    let dataPath = __dirname + '/../data';
    let mlPath = dataPath + '/' + 'ml-' + size;
    let url = (size == '1m' ? "http://files.grouplens.org/datasets/movielens/ml-1m.zip" : 
      "http://files.grouplens.org/datasets/movielens/ml-100k.zip");
    let zipFile = dataPath + '/' + (size == '1m' ? "ml-1m.zip" : "ml-100k.zip");

    return fs.existsSync(mlPath) || fs.existsSync(zipFile) ? Promise.resolve() : 
      new Promise((resolve, reject) => {
        console.log("Downloading " + url + ' ...');
        let zipFileStream = fs.createWriteStream(zipFile);
        let request = http.get(url, (response) => {
          if (response.statusCode == 200) {
            response.pipe(zipFileStream);
            zipFileStream.once('finish', () => {
              resolve();
            });
          } else reject("HTTP status is " + response.statusCode);
        });
    }).then(() => {
      return fs.existsSync(mlPath) ? Promise.resolve() : 
        new Promise((resolve, reject) => {
          console.log("Extracting " + zipFile + ' ...');
          extractZip(zipFile, {
            dir: dataPath,
          }, (err) => {
            if (!err) {
              if (fs.existsSync(mlPath)) {
                let files = fs.readdirSync(mlPath);
                for (let file of files) {
                  fs.chmodSync(mlPath + '/' + file, parseInt('0644', 8));
                }
                if (size == '1m') {
                  console.log('Replacing delimeter \'::\' to \'~\' ...');
                  for (let file of ['users.dat', 'movies.dat', 'ratings.dat']) {
                    let data = fs.readFileSync(mlPath + '/' + file, 'utf8');
                    let result = data.replace(/::/g, '~');
                    fs.writeFileSync(mlPath + '/' + file, result);
                  }
                }
                resolve();
              } else reject("Folder does not exist after unzipping: " + mlPath);
            } else reject(err);
          });
        });
    }).then(() => {
      console.log("Deleting all data ...");
      return this._deleteAllData();
    }).then(() => {
      console.log('Importing from ML ' + size + ' ...');
      let func = 'malrec_import_ml_' + size;
      return this.db.func(func, [ mlPath ]);
    }).then(() => {
      console.log('Done.');
      return;
    });
  }

  /**
   *
   */
  _deleteAllData() {
    this.emf.detachSharedFactors();
    this.emf.detachWorkPortionBuffers();
    this.emf.deleteCalcResultsSync();

    return this.db.func("malrec_delete_all_data", []);
  }

  /**
   *
   */
  deleteAllData() {
    console.log("Deleting all data ...");
    return this._deleteAllData().then(() => {
      console.log('Done.');
      return;    
    });
  }

  /**
   *
   */
  destroy() {
    if (this.emf)
      this.emf.destroy();
  }

  /**
   *
   */
  getUserByLogin(userLogin) {
    return this.db.oneOrNone("\
      select u.*, trim(u.login) as login, \
        ue.unrated_items, ue.fav_items, ue.most_rated_items \
      from malrec_users as u \
      left join malrec_users_extra as ue \
        on ue.id = u.id \
      where u.login = $(userLogin) \
    ", {
      userLogin: userLogin
    });
  }

  /**
   *
   */
  getUserById(userId) {
    return this.db.oneOrNone("\
      select u.*, trim(u.login) as login, \
        ue.unrated_items, ue.fav_items, ue.most_rated_items \
      from malrec_users as u \
      left join malrec_users_extra as ue \
        on ue.id = u.id \
      where u.id = $(userId) \
    ", {
      userId: userId
    });
  }

  /**
   * 
   */
  loadItems() {
    return Promise.resolve();

    //todo load from db   this.items = { 1: null, 2: null, .. }, also franchises
    //todo: reload when there are new items (for malrec - after task 'NewAnimes')
  }

  /**
   * 
   */
  recommendItemsForUser(user, limit = 20) {
    let emfRec = this.emf.recommender;
    let err = emfRec.getCanPredictError();
    if (err)
      return Promise.reject(err);
    return Promise.all([
      this.db.oneOrNone("\
        select array_to_string(array_agg(item_id), ',') as item_ids \
        from malrec_ratings \
        where user_list_id = $(user_list_id) \
        group by user_list_id \
      ", {
        user_list_id: user.list_id,
      })
    ]).then(([rated_items]) => {
      let ratedItemIds = rated_items ? rated_items.item_ids.split(',').map(Number) : [];
      let unratedItemsIds = user.unrated_items ? user.unrated_items.map(Number) : [];
      let skipItemIds = {};
      for (let itemId1 of ratedItemIds)
        skipItemIds[itemId1] = 1;
      for (let itemId1 of unratedItemsIds)
        skipItemIds[itemId1] = 1;

      //todo: skip missing and deleted items when looping items

      let recItems = [];
      let minRatingInSelection = 0;
      let minRecommendRating = this.minRecommendRating; //todo: take to account user's avg rating
      
      if (user.list_id) {
        let userId1 = user.list_id;
        let userId0 = userId1 - 1;
        let userFactors = emfRec.getFactorsRowSync('byUser', userId0);
        for (let itemId0 = 0 ; itemId0 < emfRec.totalItemsCount ; itemId0++) { //todo instead fetch for this.items
          let itemId1 = itemId0+1;
          if (!skipItemIds[itemId1]) {
            let predict = emfRec.predictSync(userId0, itemId0, userFactors);
            if (predict >= minRecommendRating 
              && (recItems.length < limit || predict > minRatingInSelection)) {
              recItems.push({predict: predict, id: itemId1});
              recItems.sort((a, b) => b.predict - a.predict);
              if (predict > minRatingInSelection)
                minRatingInSelection = predict;
              if (recItems.length >= limit)
                recItems.pop();
            }
          }
        }
      } else {
        //User hasn't list yet
        //We can't give him personal recommendations, only common (like most popular items)
        //todo: give most rated items
        //todo_later: social recommendations
      }

      return recItems;
    });
  }

  //todo: calc RMSE separately, not only by emf train

  //todo: saveCalcResults every iter!

  //todo: optimizations for db table malrec_ratings? see EmfLord - doSplitToSets, doUpdateStats

}
var cls = YcnrController; //for using "cls.A" as like "self::A" inside class

module.exports = YcnrController;
