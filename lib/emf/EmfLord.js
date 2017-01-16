/**
 * Master process, main node in cluster
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
const extractZip = require('extract-zip');
const shuffle = require('knuth-shuffle').knuthShuffle;



/**
 * 
 */
class EmfLord extends EmfMaster {

  /**
   *
   */
  constructor() {
    super(true);

    this.busyPortions = {};
    this.droppedPortions = [];
  }

  /**
   * @param bool forceResplitToSets - update data sets random distribution 
   *  on train/validate/test
   */
  splitDataForTrain(forceResplitToSets = false) {
    return this.splitToSets(forceResplitToSets)
      .then(() => this.getStats())
      .then(() => this.splitToPortions());
  }

  /**
   *
   */
  getStats() {
    return Promise.all([
      this.db.one("\
        select max(list_id) as max_id \
        from malrec_users \
      "),
      this.db.one("\
        select max(id) as max_id \
        from malrec_items \
      "),
      this.db.one("\
        select count(list_id) as cnt \
        from malrec_users \
        where is_used_for_train = true \
      "),
      this.db.one("\
        select count(id) as cnt \
        from malrec_items \
        where is_used_for_train = true \
      "),
      this.db.any("\
        select list_id as id, ratings_count as cnt, avg_rating as avg \
        from malrec_users \
        where is_used_for_train = true and list_id is not null \
        order by list_id \
      "),
      this.db.any("\
        select id, ratings_count as cnt, avg_rating as avg \
        from malrec_items \
        where is_used_for_train = true \
        order by id \
      "),
    ]).then(data => {
      this.totalUsersCount = parseInt(data[0].max_id);
      this.totalItemsCount = parseInt(data[1].max_id);
      this.trainUsersCount = parseInt(data[2].cnt);
      this.trainItemsCount = parseInt(data[3].cnt);
      let ratingsPerUser = data[4];
      let ratingsPerItem = data[5];

      this.stats.maxRatingsPerUser = 0;
      this.stats.maxRatingsPerItem = 0;
      this.stats.ratingsCntPerUser = [];
      this.stats.ratingsAvgPerUser = [];
      this.stats.ratingsCntPerItem = [];
      this.stats.ratingsAvgPerItem = [];
      this.stats.trainUsersRatingsCount = 0;
      for (let row of ratingsPerUser) {
        row.id = parseInt(row.id);
        row.cnt = parseInt(row.cnt);
        row.avg = parseFloat(row.avg);
        if (row.cnt == 0)
          continue;
        if (row.cnt > this.stats.maxRatingsPerUser)
          this.stats.maxRatingsPerUser = row.cnt;
        this.stats.ratingsCntPerUser[row.id - 1] = row.cnt;
        this.stats.ratingsAvgPerUser[row.id - 1] = row.avg;
        this.stats.trainUsersRatingsCount += row.cnt;
      }
      this.stats.trainItemsRatingsCount = 0;
      for (let row of ratingsPerItem) {
        row.id = parseInt(row.id);
        row.cnt = parseInt(row.cnt);
        row.avg = parseFloat(row.avg);
        if (row.cnt == 0)
          continue;
        if (row.cnt > this.stats.maxRatingsPerItem)
          this.stats.maxRatingsPerItem = row.cnt;
        this.stats.ratingsCntPerItem[row.id - 1] = row.cnt;
        this.stats.ratingsAvgPerItem[row.id - 1] = row.avg;
        this.stats.trainItemsRatingsCount += row.cnt;
      }

      console.log(
        "Users:", this.trainUsersCount,
        "\nItems:", this.trainItemsCount,
        "\nRatings:", [this.stats.trainUsersRatingsCount, this.stats.trainItemsRatingsCount]
      );
      return;
    });
  }

  /**
   * 
   */
  splitToSets(forceResplit = false) {
    let doSplit, splitAllRatings = false;
    if (forceResplit || this.lastCalcInfo 
      && !_.isEqual(this.options.dataSetDistr, this.lastCalcInfo.dataSetDistr)) {
      //distribution % changed, need to resplit
      splitAllRatings = true;
      doSplit = true;
    }
    if (this.isFirstTrain())
      splitAllRatings = true;

    let pcts = this.options.dataSetDistr;
    if (doSplit)
      console.log("Splitting" + (splitAllRatings ? " all" : " new") 
        + " ratings to sets train/validate/test ", pcts);
    let t1 = Date.now();
    let promise = !doSplit ? Promise.resolve(false) : this.doSplitToSets(splitAllRatings, pcts);
    let didSplitUpdates = false;
    return promise.then((_didSplitUpdates) => {
      didSplitUpdates = _didSplitUpdates;
      console.log("Fetching current split...");
      return this.db.any("\
        select dataset_type, count(rating) as cnt \
        from malrec_ratings \
        group by dataset_type \
      ");
    }).then((cntsByDataset) => {
      let splitCnts = {
        'train': 0,
        'validate': 0,
        'test': 0,
      };
      if (cntsByDataset)
        for (let row of cntsByDataset) {
          splitCnts[ cls.DatasetTypeI2K[row.dataset_type] ] = parseInt(row.cnt);
        }

      let total = Object.keys(splitCnts)
        .filter((typeKey) => (typeKey != 'new'))
        .map((typeKey) => splitCnts[typeKey])
        .reduce((prev, curr) => (prev + curr));
      let newPcts = [ 
        splitCnts.train / total * 100, 
        splitCnts.validate / total * 100, 
        splitCnts.test / total * 100 ];
      let isOkSplit;
      if (doSplit && splitAllRatings) {
        isOkSplit = true;
      } else {
        isOkSplit = (total > 0 && !splitCnts.not);
        for (let i = 0 ; i < 3 ; i++) {
          if ( Math.abs(newPcts[i] - pcts[i]) > 1 )
            isOkSplit = false;
        }
      }

      if (!isOkSplit) {
        return this.splitToSets(true);
      } else {
        this.stats.ratingsCountTrain = splitCnts.train + splitCnts.validate;
        this.stats.ratingsCountValidate = splitCnts.validate;
        this.stats.ratingsCountTest = splitCnts.test;

        let t2 = Date.now();
        console.log("Data are splitted to sets train/validate/test %", newPcts, 
          (", fetched in " + ((t2-t1)/1000).toFixed(1) + " s"),
          "\nTrain:", this.stats.ratingsCountTrain,
          "\nValidate:", this.stats.ratingsCountValidate,
          "\nTest:", this.stats.ratingsCountTest
        );

        //todo: Calcing avg & stats is too slow for huge MAL data.
        //Use redis to cache ratings sum+cnt per every U/I and also total?

        return Promise.all([
          this.getDbStat('totalRatingsAvg'),
          this.getDbStat('ratingsCountTrain'),
          this.getDbStat('ratingsCountValidate'),
          this.getDbStat('ratingsCountTest'),
        ]).then(([totalRatingsAvg, ratingsCountTrain, ratingsCountValidate, 
         ratingsCountTest]) => {
          if (totalRatingsAvg !== null && !didSplitUpdates 
            && (ratingsCountTrain + ratingsCountValidate + ratingsCountTest) 
             == (this.stats.ratingsCountTrain + this.stats.ratingsCountValidate 
              + this.stats.ratingsCountTest)
          ) {
            this.stats.totalRatingsAvg = parseFloat(totalRatingsAvg);
            console.log("Ratings not changed since previuos calc, can reuse ready stats.");
            console.log("Avg rating = " + this.stats.totalRatingsAvg);
          } else {
            console.log("Fecthing avg rating...");
            return this.db.oneOrNone("\
              select avg(r.rating) as avg \
              from malrec_ratings as r \
              where dataset_type in (1,2,3) \
            ").then((avgRat) => {
              let t3 = Date.now();
              this.stats.totalRatingsAvg = avgRat ? avgRat.avg : 0;
              console.log("Avg rating = " + this.stats.totalRatingsAvg 
                + ", fetched in " + ((t3-t2)/1000).toFixed(1) + " s");
              console.log("Updating stats...");
              return this.doUpdateStats()
                .then(() => Promise.all([
                  this.setDbStat('totalRatingsAvg', this.stats.totalRatingsAvg),
                  this.setDbStat('ratingsCountTrain', this.stats.ratingsCountTrain),
                  this.setDbStat('ratingsCountValidate', this.stats.ratingsCountValidate),
                  this.setDbStat('ratingsCountTest', this.stats.ratingsCountTest),
                ]))
                .then(() => {
                  let t4 = Date.now();
                  console.log("Stats updated in " + ((t4-t3)/1000).toFixed(1) + " s");
                });
            });
          }
        });
      }
    });
  }

  /**
   *
   */
  doUpdateStats() {
    /*
    return this.db.func("malrec_upd_stats", []);
    */

    let updateUsersStats = () => {
      return this.db.one("\
        select coalesce(max(list_id), 0) as max \
        from malrec_users \
      ").then((row) => {
        let maxId = row.max;
        let idsPerPortion = 1000;
        this.workProgress = new ProgressBar('For users' 
          + ' [:bar] :current/:total :elapsed/:etas', { total: Math.ceil(maxId / idsPerPortion) });
        let gotIds = 0, processedIds = 0;
        return Helpers.promiseWhile(() => (gotIds < maxId), () => {
          return this.db.manyOrNone("\
            select u.list_id, sub.new_ratings_count, sub.new_avg_rating, \
             u.ratings_count, u.avg_rating \
            from ( \
              select u.list_id, count(r.rating) as new_ratings_count, \
               avg(r.rating) as new_avg_rating \
              from malrec_users as u \
              inner join malrec_ratings as r \
               on r.user_list_id = u.list_id \
              where u.list_id > $(fromId) and u.list_id <= $(toId) \
               and u.is_used_for_train = true and r.dataset_type IN (1, 2, 3) \
              group by u.list_id \
            ) as sub \
            inner join malrec_users as u \
             on u.list_id = sub.list_id \
          ", {
            fromId: gotIds,
            toId: (gotIds + idsPerPortion),
          }).then((rows) => {
            gotIds += idsPerPortion;
            let updatedStats = {};
            if (rows)
              for (let row of rows) {
                row.new_ratings_count = parseFloat(row.new_ratings_count);
                row.new_avg_rating = parseFloat(row.new_avg_rating);
                let cntChanged = (row.new_ratings_count.toFixed(6) 
                  != row.ratings_count.toFixed(6));
                let avgChanged = (row.new_avg_rating.toFixed(6) != row.avg_rating.toFixed(6));
                if (cntChanged || avgChanged) {
                  updatedStats[row.list_id] = {};
                  if (cntChanged)
                    updatedStats[row.list_id]['ratings_count'] = row.new_ratings_count;
                  if (avgChanged)
                    updatedStats[row.list_id]['avg_rating'] = row.new_avg_rating;
                }
              }

            let updQueries = [];
            if (Object.keys(updatedStats).length) {
              for (let list_id in updatedStats) {
                let upds = updatedStats[list_id];
                let params = {}, set = "";
                for (let k in upds) {
                  set += (set != '' ? ', ' : '') + k + " = $("+k+")";
                  params[k] = upds[k];
                }
                params['list_id'] = list_id;
                updQueries.push(this.db.query("\
                  update malrec_users \
                  set "+ set +" \
                  where list_id = $(list_id) \
                ", params));
              }
            }
            return Promise.all(updQueries).then(() => {
              this.workProgress.tick();
            });
          });
        });
      });
    };

    let updateItemsStats = () => {
      console.log("For items - fetching all cnt/avg...");
      let t1 = new Date();
      return this.db.manyOrNone("\
        select i.id, sub.new_ratings_count, sub.new_avg_rating, \
         i.ratings_count, i.avg_rating \
        from ( \
          select i.id, count(r.rating) as new_ratings_count, \
           avg(r.rating) as new_avg_rating \
          from malrec_items as i \
          inner join malrec_ratings as r \
           on r.item_id = i.id \
          where i.is_used_for_train = true and r.dataset_type IN (1, 2, 3) \
          group by i.id \
        ) as sub \
        inner join malrec_items as i \
         on i.id = sub.id \
      ").then((rows) => {
        let t2 = new Date();
        console.log("For items - fetched in " + ((t2-t1)/1000).toFixed(1) + " s");
        let updatedStats = {};
        if (rows)
          for (let row of rows) {
            row.new_ratings_count = parseFloat(row.new_ratings_count);
            row.new_avg_rating = parseFloat(row.new_avg_rating);
            let cntChanged = (row.new_ratings_count.toFixed(6) 
              != row.ratings_count.toFixed(6));
            let avgChanged = (row.new_avg_rating.toFixed(6) != row.avg_rating.toFixed(6));
            if (cntChanged || avgChanged) {
              updatedStats[row.id] = {};
              if (cntChanged)
                updatedStats[row.id]['ratings_count'] = row.new_ratings_count;
              if (avgChanged)
                updatedStats[row.id]['avg_rating'] = row.new_avg_rating;
            }
          }

        let updQueries = [];
        if (Object.keys(updatedStats).length) {
          for (let id in updatedStats) {
            let upds = updatedStats[id];
            let params = {}, set = "";
            for (let k in upds) {
              set += (set != '' ? ', ' : '') + k + " = $("+k+")";
              params[k] = upds[k];
            }
            params['id'] = id;
            updQueries.push(this.db.query("\
              update malrec_items \
              set "+ set +" \
              where id = $(id) \
            ", params).then(() => {
              this.workProgress.tick();
            }));
          }
        }
        this.workProgress = new ProgressBar('For items - updating' 
          + ' [:bar] :current/:total :elapsed/:etas', { total: updQueries.length });
        return Promise.all(updQueries).then(() => {
        });
      });
    };

    return updateUsersStats().then(() => updateItemsStats());
  }

  /**
   * @return Promise (bool) - did any updates?
   */
  doSplitToSets(splitAll, pcts) {
    /*
    let func = splitAll ? 'malrec_resplit_to_sets' : 'malrec_split_more_to_sets';
    return this.db.func(func, [ pcts[0], pcts[1], pcts[2] ]).then(() => { return true; });
    */

    //todo: complete resplit is too slow (1hr for MAL data!). Any optimizations?

    //todo: при изменении % удалить пометки о dataset с тех, % которых стали <
    // для splitAll - все пометки

    return this.db.one("\
      select coalesce(max(list_id), 0) as max \
      from malrec_users \
    ").then((row) => {
      let maxId = row.max;
      let idsPerPortion = 100;
      this.workProgress = new ProgressBar('Splitting' 
        + ' [:bar] :current/:total :elapsed/:etas', { total: Math.ceil(maxId / idsPerPortion) });
      let gotIds = 0, processedIds = 0;
      let didUpdates = false;
      return Helpers.promiseWhile(() => (gotIds < maxId), () => {
        return this.db.manyOrNone("\
          select user_list_id, array_agg(item_id) filter (where dataset_type = 0) as item_ids, \
            count(rating) filter (where dataset_type = 1) as train_cnt, \
            count(rating) filter (where dataset_type = 2) as validate_cnt, \
            count(rating) filter (where dataset_type = 3) as test_cnt \
          from malrec_users as u \
          inner join malrec_ratings as r \
           on r.user_list_id = u.list_id \
          where u.list_id > $(fromId) and u.list_id <= $(toId) \
           and u.is_used_for_train = true and r.dataset_type <> 4 \
          group by user_list_id \
          order by user_list_id \
        ", {
          fromId: gotIds,
          toId: (gotIds + idsPerPortion),
        }).then((ratsPerList) => {
          gotIds += idsPerPortion;
          let idsToSet = {};
          if (ratsPerList) {
            for (let row of ratsPerList) {
              row.train_cnt = parseInt(row.train_cnt);
              row.validate_cnt = parseInt(row.validate_cnt);
              row.test_cnt = parseInt(row.test_cnt);
              let freeIds = row.item_ids;
              if (freeIds && freeIds.length) {
                let totalCnt = freeIds.length + row.train_cnt + row.validate_cnt 
                 + row.test_cnt;
                let targetCnts = [];
                targetCnts[0] = Math.ceil(totalCnt * pcts[0] / 100);
                targetCnts[1] = Math.ceil(totalCnt * (pcts[0] + pcts[1]) / 100) - targetCnts[0];
                targetCnts[2] = totalCnt - (targetCnts[0] + targetCnts[1]);
                let newCnts = [];
                newCnts[0] = Math.max(0, targetCnts[0] - row.train_cnt);
                newCnts[1] = Math.max(0, targetCnts[1] - row.validate_cnt);
                newCnts[2] = Math.max(0, targetCnts[2] - row.test_cnt);
                if ((newCnts[0] + newCnts[1] + newCnts[2]) < freeIds.length) {
                  newCnts[0] += (freeIds.length - (newCnts[0] + newCnts[1] + newCnts[2]));
                }
                shuffle(freeIds);
                let idsBySet = {};
                let offs = 0;
                for (let i = 0 ; i < 3 ; i++) {
                  let dataset_type = (i+1);
                  if (newCnts[i]) {
                    let ids = freeIds.slice(offs, offs + newCnts[i]);
                    idsBySet[dataset_type] = ids;
                    offs += newCnts[i];
                  }
                }
                idsToSet[row.user_list_id] = idsBySet;
              }
            }
          }

          let updQueries = [];
          if (Object.keys(idsToSet).length) {
            for (let user_list_id in idsToSet) {
              for (let dataset_type in idsToSet[user_list_id]) {
                let itemIds = idsToSet[user_list_id][dataset_type];
                updQueries.push(this.db.query("\
                  update malrec_ratings \
                  set dataset_type = $(dataset_type) \
                  where user_list_id = $(user_list_id) and item_id in("+ itemIds.join(', ') +") \
                ", {
                  dataset_type: dataset_type,
                  user_list_id: user_list_id,
                }));
              }
            }
          }
          return Promise.all(updQueries).then(() => {
            this.workProgress.tick();
            if (updQueries.length > 0)
              didUpdates = true;
          });
        });
      }).then(() => {
        return didUpdates;
      });
    });
  }

  /**
   * 
   */
  splitToPortions() {
    if (this.stats.trainUsersRatingsCount == 0 || this.stats.trainItemsRatingsCount == 0)
      return Promise.resolve();

    return new Promise((resolve, reject) => {
      this.portionsRowIdTo = {};
      
      // Calc optimal distribution of portions of data to calc in parallel

      // if 100.000 ratings and 1.000 users (rows), then avg 100 ratings per row
      // if ratingsInPortion = 5.000, then will be 20 portions, 50 rows in portion
      //todo_later: also consider that portion with 1 item and 1000 ratings will be calced
      // faster than portion with 100 items and 10 ratings per each
      let steps = this.options.alg == 'als' ? ['byUser', 'byItem', 'rmseValidate', 'rmseTest'] 
        : ['rmseValidate', 'rmseTest'];
      for (let stepType of steps) {
        let rowsCnt = (stepType == "byItem" ? this.trainItemsCount : this.trainUsersCount);
        let ratingsCntPer = (stepType == "byItem" ? this.stats.ratingsCntPerItem
          : this.stats.ratingsCntPerUser);
        let maxRatingsPerRow = (stepType == "byItem" ? this.stats.maxRatingsPerItem
          : this.stats.maxRatingsPerUser);
        let ratingsCount = (stepType == "byItem" ? this.stats.trainItemsRatingsCount
          : this.stats.trainUsersRatingsCount);

        if (stepType == "rmseTest") {
          //tip: take 1% more to fit in buffers
          ratingsCount = Math.ceil(ratingsCount * 
            ((this.options.dataSetDistr[2] + 1) / 100));
          maxRatingsPerRow = Math.ceil(maxRatingsPerRow * 
            ((this.options.dataSetDistr[2] + 1) / 100));
        } else if(stepType == "rmseValidate") {
          ratingsCount = Math.ceil(ratingsCount * 
            ((this.options.dataSetDistr[1] + 1) / 100));
          maxRatingsPerRow = Math.ceil(maxRatingsPerRow * 
            ((this.options.dataSetDistr[1] + 1) / 100));
        } else {
          //tip: for ALS ignore split to datasets, train+validate should be close to 100%,
          // so take 100% for calcs of portions
        }

        let ratingsInPortion = (stepType == "rmseValidate" || stepType == "rmseTest" 
          ? this.options.ratingsInPortionForRmse 
          : this.options.ratingsInPortionForAls[stepType]);
        let avgPortionsCount = Math.ceil(ratingsCount / ratingsInPortion);
        let avgRowsInPortion = Math.floor(rowsCnt / avgPortionsCount);
        if (avgPortionsCount < this.options.numThreadsForTrain[this.options.alg]) {
          avgPortionsCount = this.options.numThreadsForTrain[this.options.alg];
          ratingsInPortion = Math.ceil(ratingsCount / avgPortionsCount);
          avgRowsInPortion = Math.floor(rowsCnt / avgPortionsCount);
        }
        if (avgRowsInPortion < 1) {
          avgRowsInPortion = 1;
          avgPortionsCount = rowsCnt;
          ratingsInPortion = Math.ceil(ratingsCount / avgPortionsCount);
        }
        if (ratingsInPortion < maxRatingsPerRow) {
          ratingsInPortion = maxRatingsPerRow;
          avgPortionsCount = Math.ceil(ratingsCount / ratingsInPortion);
          avgRowsInPortion = Math.floor(rowsCnt / avgPortionsCount);
        }

        let portionsRowIdTo = [];
        let p = 0, rtgs = 0, rows = 0, maxRows = 0;
        for (let id in ratingsCntPer) {
          let cnt = ratingsCntPer[id];
          if (stepType == "rmseTest") {
            cnt = Math.ceil(cnt * ((this.options.dataSetDistr[2] + 1) / 100));
          } else if (stepType == "rmseValidate") {
            cnt = Math.ceil(cnt * ((this.options.dataSetDistr[1] + 1) / 100));
          }

          id = parseInt(id);
          if ((rtgs + cnt) > ratingsInPortion) {
            rtgs = 0;
            rows = 0;
            p++;
          }
          rtgs += cnt;
          rows++;
          if (rows > maxRows)
            maxRows = rows;
          portionsRowIdTo[p] = id + 1; //id is 0-based, for db we need 1-based
        }

        this.portionsRowIdTo[stepType] = portionsRowIdTo;
        this.portionsCount[stepType] = portionsRowIdTo.length;
        this.maxRatingsInPortion[stepType] = ratingsInPortion;
        this.maxRowsInPortion[stepType] = maxRows;
      }
      console.log(
        "Work portions:", this.portionsCount,
        "\nMax ratings in portion:", this.maxRatingsInPortion, 
        "\nMax rows in portion:", this.maxRowsInPortion
      );
      
      if (this.options.alg == 'sgd') {
        this.portionsCount['sgd'] = Math.ceil(this.stats.ratingsCountTrain / 
          this.options.ratingsInPortionForSgd);
      }

      resolve();
    });
  }

  /**
   * @param bool forceResplitToSets false to use previous data sets distribution
   */
  prepareToTrain(forceResplitToSets = false) {
    console.log("Preparing for train..");
    // Once prepare db for train
    // Create `rand` column in `malrec_ratings` table for SGD, else remove it
    let func = this.options.alg == 'sgd' ? 'malrec_add_ratings_rand' 
      : 'malrec_drop_ratings_rand';
    return this.db.func(func, []).then(() => {
      // Fix date
      return this.db.one("select now() as now")
    }).then((data) => {
      this.calcDate = data.now;
      //tip: Need to train always all users/items.
      // Even when retraining after some ratings changed, don't train only affected users and 
      // items, because that changes can potentially  cause "butterfly effect" on U/I factors
      this.options.trainAllUsersItems = true;
      //let trainAllUsersItems = (this.options.alg == 'sgd' 
      // || !this.lastCalcInfo || this.lastCalcInfo 
      // && !_.isEqual(this.options.dataSetDistr, this.lastCalcInfo.dataSetDistr));
      console.log("Fixing before train...");
      return this.db.func('malrec_fix_for_train', [this.options.trainAllUsersItems]);
    }).then((ratingsModified) => {
      ratingsModified = (ratingsModified > 0);
      return this.splitDataForTrain(forceResplitToSets);
    }).then(() => {
      if (this.trainUsersCount == 0 && this.trainItemsCount == 0)
        return Promise.reject({"code": "no_data", "error": "No data to train"});
      console.log('Preparing factors...');
      return this.prepareSharedFactors();
    }).then(() => {
      console.log('Preparing workers...');
      return this.prepareWorkersToTrain();
    }).then(() => {
      console.log("Total shared memory used: " + Helpers.humanFileSize(shm.getTotalSize()));
      console.log('Prepared to train');
      return;
    });
  }
  
  /**
   *
   */
  initCluster() {
    if (this.options.useClustering)
      return this.initClusterLord();
    else
      return Promise.resolve();
  }

  /**
   *
   */
  initClusterLord() {
    this.clusterNodes = {}; //see this.gatherClusterNodes()
    this.myClients = {};
    var lastNodeId = 1;

    this.lordServer = net.createServer({}, (clientSocket) => {
      let client = new TcpSocket(true, clientSocket);
      let nodeId = 's' + (lastNodeId++);

      client.on("register", (data) => {
        if (this.status != "ready") {
          client.destroy("Sorry, server state is " + this.status + ", try again later");
        } else {
          client.data.nodeId = nodeId;
          client.data.host = data.host;
          client.data.port = data.port;
          this.myClients[nodeId] = client;
          console.log("Cluster node #" + nodeId + " registered: ", data);
          client.sendPacket("registered", {
            nodeId: nodeId
          });
        }
      })

      client.on('error', (err) => {
        console.log("Cluster node #" + nodeId + " client socker error: ", err);
      });

      client.on('close', () => {
        if (this.myClients[nodeId]) {
          console.log("Cluster node #" + nodeId + " disconnected." 
            + (this.busyPortions[nodeId] ? " He left unfinished portions: " 
            + this.busyPortions[nodeId] : ""));
          delete this.myClients[nodeId];
          if (this.busyPortions[nodeId]) {
            this.droppedPortions.push(...this.busyPortions[nodeId]);
            delete this.busyPortions[nodeId];
          }
          this.m_prepareNextPortions();
        }
      });

      client.on('getStats', () => {
        client.sendPacket('setStats', {
          stats: this.stats,
          options: _.pick(this.options, 
            'als', 'useDoublePrecision', 'factorsCount', 'trainIters', 'trainAllUsersItems')
        });
      });

      client.on('getNextPortions', (data) => {
        this.m_incrNextPortion(data.wantPortions, (portions) => {
          this.busyPortions[nodeId].push(...portions);
          client.sendPacket('retNextPortions_'+data.reqId, {
            portions: portions
          });
        });
      });

      client.on("alsSaveCalcedFactors", (data) => {
        let stream = this.getWriteStreamToSaveCalcedFactors(this.stepType, data.rowsRange);
        client.receiveStreams([stream], () => {
          this.m_completedPortion(data, client.data.nodeId);
        });
      });

      client.on('rmseSaveCalcs', (data) => {
        this.m_completedPortion(data);
      });

      client.on('getFactors', (data) => {
        client.sendPacketAndStreams('setFactors', null, this.getFactorsReadStreams());
      });
    });
    this.lordServer.listen(this.options.clusterServerPort, () => {
      console.log("Listening cluster on port " + this.options.clusterServerPort);
    });

    return Promise.resolve();
  }

  /**
   * 
   */
  gatherClusterNodes() {
    if (!this.options.useClustering)
      return Promise.resolve();
    else return new Promise((resolve, reject) => {
      if (Object.keys(this.myClients).length)
        console.log('Let cluster slaves to connect with each other (full-mesh)...');

      let meetCnt = Object.keys(this.myClients).length;
      let metCnt = 0;
      let connectCnt = Object.keys(this.myClients).length;
      let connectedCnt = 0;
      let checkTotalMetCnt = () => {
        if (metCnt == meetCnt && connectedCnt == connectCnt) {
          if (connectedCnt)
            console.log('Cluster is gathered: ' + connectedCnt + ' slaves');
          resolve();
        }
      }

      for (let clientId in this.myClients) {
        let client = this.myClients[clientId];
        let meetNodes = {};
        for (let _clientId in this.myClients) {
          if (_clientId !== clientId) {
            let _client = this.myClients[_clientId];
            meetNodes[_clientId] = {
              host: _client.data.host,
              port: _client.data.port
            };
          }
        }
        client.sendPacket("meetCluster", {
          nodes: meetNodes
        }, () => {
          client.once("metCluster", () => {
            metCnt++;
            checkTotalMetCnt();
          });
        });
      }

      for (let nodeId in this.myClients) {
        let _client = this.myClients[nodeId];
        if (this.clusterNodes[nodeId]) {
          //already connected
          connectedCnt++;
        } else {
          let nodeClient = null;
          let nodeClientSocket = net.connect(_client.data.port, _client.data.host, () => {
            nodeClient = new TcpSocket(false, nodeClientSocket);

            nodeClient.sendPacket("setMyNodeId", {
              "nodeId": 'm'
            }, () => {
              nodeClient.once("gotYourNodeId", () => {
                console.log("Connected to cluster node #" + nodeId);
                this.clusterNodes[nodeId] = nodeClient;
                connectedCnt++;
                checkTotalMetCnt();
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

      checkTotalMetCnt();
    });
  }

  /**
   *
   */
  prepareClusterToTrain() {
    if (!this.options.useClustering || Object.keys(this.clusterNodes).length == 0)
      return Promise.resolve();
    else {
      console.log("Preparing cluster nodes to train...");
      this.broadcastMessageToCluster("prepareToTrain", {
        lastCalcDate: this.lastCalcDate
      });
      return this.whenGotMessageFromCluster("preparedToTrain").then(() => {
        console.log('Cluster nodes are ready to train with me.');
        return;
      });
    }
  }

  /**
   * 
   */
  getCanTrainError() {
    let err = null;
    if(this.status == "training") {
      err = "Training is already started";
    } else if(this.status != "ready") {
      err = "Not ready to train. Status is " + this.status;
    }
    return err;
  }

  /**
   * 
   */
  train() {
    let err = this.getCanTrainError();
    if (err !== null) {
      return Promise.reject(err);
    } else {
      this._status = "preparing";
      return this.prepareToTrain().catch(err => {
        if (err.code == "no_data") {
          console.log("Unfixing after train...");
          //tip: malrec_fix_for_train was called at prepareToTrain()
          return this.db.func('malrec_unfix_for_train', [this.options.trainAllUsersItems])
          .then(() => {
            console.log("No data to train");
            this._status = "ready";
            return Promise.reject(err);
          });
        } else throw err;
      }).then(() => {
        this._status = "gathering";
        return this.gatherClusterNodes();
      }).then(() => {
        this._status = "syncing";
        return this.prepareClusterToTrain();
      }).then(() => {
        console.log('*** Starting train...');
        this._status = "training";
        this.broadcastMessageToCluster("startTrain");
        this.trainIter = 0;
        return Helpers.promiseWhile(() => (this.trainIter < this.options.trainIters), () => {
          //console.log('>>> Training iteration #' + this.trainIter + ' ...');
          return this.initTrainIter()
          .then(() => (this.options.alg == 'als' ? this.alsTrainIter() : this.sgdTrainIter()))
          .then(() => this.calcRmse('rmseValidate', false))
          .then(() => this.calcRmse('rmseTest', false))
          .then(() => this.calcRmse('rmseTest', true))
          .then(() => {
            this.trainIter++;
          });
        });
      }).then(() => {
        this.calcCnt++;
        console.log('Saving calc results...');
        this.broadcastMessageToWorkers("endTrain");
        this.broadcastMessageToCluster("endTrain", {
          info: this.getCalcInfo(),
        });
        this.detachWorkPortionBuffers();
        return Promise.all([
          this.whenGotMessageFromCluster("endedTrain"),
          this.saveCalcResults(this.getCalcInfo())
        ]).then(() => {
          //tip: malrec_fix_for_train was called at prepareToTrain()
          console.log("Unfixing after train...");
          return this.db.func('malrec_unfix_for_train', [this.options.trainAllUsersItems])
          .then(() => {
            this._status = "ready";
            console.log('Peak memory usage: ' + this.formatPeakMemoryUsage());
            console.log('*** Training complete');
          });
        });
      });
    }
  }

  /**
   *
   */
  initTrainIter() {
    if (this.options.alg == 'sgd') {
      //is there better option?
      //randomizing indexes in memory can be quicker, but requires a LOT of memory
      console.log('Randomizing ratings order...');
      let t1 = Date.now();
      return this.db.query("\
        update malrec_ratings \
        set rand = random() * 4294967294 - 2147483647 \
        where dataset_type in (1, 2) \
      ").then(() => {
        let t2 = Date.now();
        console.log('Randomizing ratings order done in ' + (t2-t1) + 'ms');
        return;
      });
    } else {
      return Promise.resolve();
    }
  }

  /**
   *
   */
  alsTrainIter() {
    //2 steps - first fix item vectrors and calc user vectors, then vice versa
    return this.alsTrainStep('byUser')
      .then(() => this.alsTrainStep('byItem'));
  }

  /**
   * @param string stepType 'byUser', 'byItem'
   */
  alsTrainStep(stepType) {
    this.nextPortion = 0;
    this.busyPortions = {};
    for (let nodeId in this.clusterNodes) {
      this.busyPortions[nodeId] = [];
    }
    this.droppedPortions = [];

    this._startAlsTrainStep(stepType);

    this.broadcastMessageToCluster("startAlsTrainStep", {
      trainIter: this.trainIter,
      stepType: stepType,
    });

    return new Promise((resolve, reject) => {
      this.eventEmitter.once('stepComplete' , () => {
        this.broadcastMessageToCluster("alsTrainStepEnd");
        resolve();
      });
    });
  }

  /**
   * 
   */
  _incrNextPortion(wantPortions, callback) {
    this.m_incrNextPortion(wantPortions, callback);
  }

  /**
   * 
   */
  m_incrNextPortion(wantPortions, callback) {
    let portions = [];
    let totalPortions = this.portionsCount[this.stepType];
    while (portions.length < wantPortions && this.droppedPortions.length) {
      portions.push(this.droppedPortions.shift());
    }
    while (this.nextPortion < totalPortions && portions.length < wantPortions) {
      portions.push(this.nextPortion++);
    }
    callback(portions);
  }

  /**
   *
   */
  sgdTrainIter() {    
    this.workType = 'train';
    this.usedThreads = this.options.numThreadsForTrain[this.options.alg];
    this.stepType = 'sgd';
    this.nextPortion = 0;
    this.completedPortions = 0;
    this.requestingPortions = 0;
    let totalPortions = this.portionsCount[this.stepType];
    this.workProgress = new ProgressBar('Iter #' + this.trainIter 
        + ' [:bar] :current/:total :elapsed/:etas', { total: totalPortions });

    for (let t = 0 ; t < this.usedThreads ; t++) {
      let w = this.workers[t];
      w.emit('startTrainStep', {
        stepType: this.stepType 
      });
    }

    this.m_prepareNextPortions();

    return new Promise((resolve, reject) => {
      this.eventEmitter.once('stepComplete' , () => {
        resolve();
      });
    });
  }



  /**
   * @param string stepType 'rmseValidate', 'rmseTest'
   */
  calcRmse(stepType, useGlobalAvgShift) {
    if (useGlobalAvgShift && this.options.alg != 'als')
       return Promise.resolve();
    if (this.options.dataSetDistr[1] == 0 && stepType == 'rmseValidate')
       return Promise.resolve();
    if (this.options.dataSetDistr[2] == 0 && stepType == 'rmseTest')
       return Promise.resolve();

    this.nextPortion = 0;
    this.busyPortions = {};
    for (let nodeId in this.clusterNodes) {
      this.busyPortions[nodeId] = [];
    }
    this.droppedPortions = [];

    this.rSum = 0;
    this.rSumDiff2 = 0;
    this.rCnt = 0;

    this._startCalcRmse(stepType, useGlobalAvgShift);

    this.broadcastMessageToCluster("startCalcRmse", {
      stepType: stepType,
      useGlobalAvgShift: useGlobalAvgShift,
    });

    return new Promise((resolve, reject) => {
      this.eventEmitter.once('stepComplete' , () => {
        this._endCalcRmse(this.globalAvgShift);
        this.broadcastMessageToCluster("endCalcRmse", {
          globalAvgShift: this.globalAvgShift,
        });
        console.log(stepType + ' = ' + this.rmse 
          + (useGlobalAvgShift ? ' (applied shift ' + this.globalAvgShift + ')' : '')
          + (this.calcGlobalAvgShift ? ' (shift = ' + this.globalAvgShift + ')' : ''));
        resolve();
      });
    });
  }

}
var cls = EmfLord; //for using "cls.A" as like "self::A" inside class

module.exports = EmfLord;
