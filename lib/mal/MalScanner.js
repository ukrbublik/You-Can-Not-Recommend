/**
 * MAL scanner
 * Grabs animes, users, anime lists to db.
 *
 * @author ukrbublik
 */


const deepmerge = require('deepmerge');
const assert = require('assert');
const _ = require('underscore')._;
const ProgressBar = require('progress');
const MalParser = require('./MalParser');
const MalError = require('./MalError');
const MalBaseScanner = require('./MalBaseScanner');
const Helpers = require('../Helpers');
const shuffle = require('knuth-shuffle').knuthShuffle;


/**
 * 
 */
class MalScanner extends MalBaseScanner {
  /**
   * @return array default options
   */
  static get DefaultOptions() {
    return {
      scanner: {
        approxBiggestUserId: 5910700, //manually biggest found user id
        maxNotFoundUserIdsToStop: 300,
        //maxNotFoundAnimeIdsToStop: 100,
        cntErrorsToStopGrab: 20,
        saveProcessingIdsAfterEvery: 50,
        log: true,
      },
      processer: {
      },
    }
  }

  constructor() {
    super();
  }

  /**
   *
   */
  init(config, options = {}) {
    config = deepmerge.all([ cls.DefaultOptions, config ]);
    return super.init(config, options).then(() => {
    });
  }

  /**
   *
   */
  checkDbConnection() {
    return this.db.proc('version');
  }


  /**
   * <task> => [<func>, <params>]
   */
  static get allTasksFuncs() {
    return {
      GenresOnce: ['grabGenresOnce'],
      NewAnimes: ['grabNewAnimes'],
      NewAnimesUserrecs: ['grabAnimesUserrecs', true],
      AllAnimesUserrecs: ['grabAnimesUserrecs', false],
      NewUserLogins: ['grabNewUserLogins'],
      NewUserProfiles: ['grabUserProfiles', true],
      AllUserProfiles: ['grabUserProfiles', false],
      NewUserLists: ['grabUserLists', true],
      AllUserLists: ['grabUserLists', false],
      UpdatedUserLists: ['grabUserLists', false, true],
      UserListsUpdated: ['grabUserListsUpdated'],
      test1: ['grabTest'],
    };
  }

  /**
   * List of tasks to grab only data for new animes and new users
   */
  static get grabNewsTasksKeys() {
    return [
      'GenresOnce',
      'NewAnimes',
      'NewAnimesUserrecs',
      'NewUserLogins',
      'NewUserLists',
      'NewUserProfiles',
    ];
  }

  /**
   * List of tasks to regrab data to check udpates
   * Do it at "cron" every N days
   */
  static get grabUpdatesTasksKeys() {
    return [
      //better to run frequently, once in day maybe
      //'AllUserLists', //too slow, better UserListsUpdated + UpdatedUserLists
      'UserListsUpdated', //can run several times, then once UpdatedUserLists when need
      'UpdatedUserLists', //after 'UserListsUpdated'
      
      'AllAnimesUserrecs', //run it rarely, like once in week..

      'AllUserProfiles', //just to update favs; run it very rarely!
    ];
  }

  /**
   *
   */
  doGrabTask(taskKey) {
    let funcAndParams = cls.allTasksFuncs[taskKey];
    if (funcAndParams === undefined)
      throw new Error("Unknown task " + taskKey);
    let func = funcAndParams[0];
    let params = funcAndParams.splice(1);
    return this[func](...params);
  }

  /**
   *
   */
  static shouldSkipTask(taskKey) {
    if (taskKey == 'GenresOnce') {
      //Skip if already grabbed
      return this.db.one("\
        select coalesce(count(id), 0) as cnt \
        from malrec_genres \
      ").then((row) => {
        return (row.cnt > 0);
      });
    } else {
      return Promise.resolve(false);
    }
  }

  /**
   *
   */
  grabTest() {
    return this.grabByIds({
      key: 'test1',
      trackBiggestId: false,
      //maxNotFoundIdsToStop: 12,
      approxMaxId: 1000,
      isByListOfIds: false,
      totalIdsCnt: () => 1000,
      getNextIds: (nextId, limit) => {
        let ids = Array.from({length: Math.min(limit, 1000 - nextId + 1)}, 
          (v, k) => nextId + k);
        return { ids: ids };
      },
      getListOfIds: () => {
        return Array.from({length: 1000}, (v, k) => 1 + k);
      },
      getDataForIds: (ids) => null,
      fetch: (id) => {
        return this.provider.queue.add(() => new Promise((resolve, reject) => {
          setTimeout(() => {
            if (Math.random() < 0.3)
              reject("random err");
            //if (id >= 2000)
            //  resolve(null);
            resolve({id: id});
          }, Helpers.getRandomInt(20,200));
        }));
      },
      process: (id, obj) => { return Promise.resolve(); },
    });
  }


  /**
   *
   */
  grabGenresOnce() {
    let key = 'GenresOnce';
    return this.provider.getGenres().then((genres) => {
      return this.processer.processGenres(genres);
    }).then(() => {
      return {};
    });
  }

  /**
   * 
   */
  grabNewAnimes() {
    let key = 'NewAnimes';
    let newMaxGrabbedAnimeId;
    return Promise.all([
      this.provider.getApproxMaxAnimeId({}),
      this.redis.getAsync("mal.maxGrabbedAnimeId")
    ]).then(([approxMaxAnimeId, maxGrabbedAnimeId]) => {
      maxGrabbedAnimeId = parseInt(maxGrabbedAnimeId);
      approxMaxAnimeId = Math.max(maxGrabbedAnimeId, approxMaxAnimeId);
      newMaxGrabbedAnimeId = approxMaxAnimeId;
      return this.grabByIds({
        key: key,
        isByListOfIds: false,
        startFromId: maxGrabbedAnimeId + 1,
        approxMaxId: approxMaxAnimeId,
        getNextIds: (nextId, limit) => {
          let ids = Array.from({length: Math.min(limit, approxMaxAnimeId - nextId + 1)}, 
            (v, k) => nextId + k);
          return { ids: ids };
        },
        getListOfIds: () => {
          return (Array.from({length: approxMaxAnimeId}, (v, k) => maxGrabbedAnimeId + 1 + k));
        },
        totalIdsCnt: () => (approxMaxAnimeId - maxGrabbedAnimeId),
        getDataForIds: (ids) => null,
        fetch: (id) => this.provider.getAnimeInfo({animeId: id}),
        process: (id, obj) => this.processer.processAnime(id, obj),
      }).then((res) => {
        this.redis.set("mal.maxGrabbedAnimeId", newMaxGrabbedAnimeId);
        return res;
      });
    });
  }


  /**
   * onlyNew - only for animes with never checked yet userrecs
   */
  grabAnimesUserrecs(onlyNew = true) {
    let key = (!onlyNew ? 'AllAnimesUserrecs' : 'NewAnimesUserrecs');
    return this.grabByIds({
      key: key,
      isByListOfIds: false,
      getNextIds: (nextId, limit) => {
        return this.db.manyOrNone("\
          select id \
          from malrec_items \
          where id >= $(nextId) \
          " + (onlyNew ? " and recs_check_ts is null " : "") + "\
          order by id asc \
          limit $(limit) \
        ", {
          nextId: nextId,
          limit: limit,
        }).then((rows) => {
          return {ids: !rows ? [] : rows.map((row) => parseInt(row.id)) };
        });
      },
      getListOfIds: () => this.db.manyOrNone("\
        select id \
        from malrec_items \
        where 1=1 \
        " + (onlyNew ? " and recs_check_ts is null " : "") + "\
        order by " + (onlyNew ? "id asc" : "recs_update_ts desc") + " \
      ", {
      }).then((rows) => {
        return !rows ? [] : (rows.map((row) => parseInt(row.id)));
      }),
      totalIdsCnt: () => this.db.one("\
        select count(*) as cnt \
        from malrec_items \
        where 1=1 \
        " + (onlyNew ? " and recs_check_ts is null " : "") + "\
      ").then((row) => row.cnt),
      getDataForIds: (ids) => null,
      fetch: (id) => this.provider.getAnimeUserrecs({animeId: id}),
      process: (id, obj) => this.processer.processAnimeUserrecs(id, obj),
    });
  }

  /**
   *
   */
  grabNewUserLogins() {
    let key = 'NewUserLogins';
    return this.redis.getAsync("mal.maxGrabbedUserId").then((maxGrabbedUserId) => {
      maxGrabbedUserId = parseInt(maxGrabbedUserId);
      if (maxGrabbedUserId == 0) {
        //first grab
        let approxMaxId = this.options.scanner.approxBiggestUserId;
        return this.grabByIds({
          key: key,
          isByListOfIds: false,
          startFromId: maxGrabbedUserId + 1,
          approxMaxId: approxMaxId,
          getNextIds: (nextId, limit) => { 
            let ids = Array.from({
              length: Math.min(limit, approxMaxId - nextId + 1)
            }, (v, k) => nextId + k);
            return { ids: ids };
          },
          getListOfIds: () => (Array.from({
            length: approxMaxId}, (v, k) => maxGrabbedUserId + 1 + k)),
          totalIdsCnt: () => (approxMaxId - maxGrabbedUserId),
          getDataForIds: (ids) => null,
          fetch: (id) => this.provider.userIdToLogin({userId: id}),
          process: (id, obj) => this.processer.processUserIdToLogin(id, obj),
        }).then((res) => {
          let newMaxGrabbedUserId = approxMaxId;
          this.redis.set("mal.maxGrabbedUserId", newMaxGrabbedUserId);
          return res;
        });
      } else {
        //not first grab
        let approxMaxId = maxGrabbedUserId;
        return this.grabByIds({
          key: key,
          trackBiggestId: true,
          isByListOfIds: false,
          startFromId: maxGrabbedUserId + 1,
          approxMaxId: approxMaxId,
          maxNotFoundIdsToStop: this.options.scanner.maxNotFoundUserIdsToStop,
          totalIdsCnt: () => (0), //unknown max
          getDataForIds: (ids) => null,
          fetch: (id) => this.provider.userIdToLogin({userId: id}),
          process: (id, obj) => this.processer.processUserIdToLogin(id, obj),
        }).then((res) => {
          if (res.biggestFoundId) {
            let newMaxGrabbedUserId = res.biggestFoundId;
            this.redis.set("mal.maxGrabbedUserId", newMaxGrabbedUserId);
          }
          if (res.cntNotFoundIdsAfterBiggest < this.options.scanner.maxNotFoundUserIdsToStop)
            res.retry = true;
          return res;
        });
      }
    });
  }

  /**
   * onlyNew - only for users with missing profile data
   */
  grabUserProfiles(onlyNew = true) {
    let key = (onlyNew ? 'NewUserProfiles' : 'AllUserProfiles');
    return this.grabByIds({
      key: key,
      isByListOfIds: false,
      getNextIds: (nextId, limit) => {
        return this.db.manyOrNone("\
          select id, login \
          from malrec_users \
          where id >= $(nextId) \
          " + (onlyNew ? " and reg_date is null " : "") + "\
          order by id asc \
          limit $(limit) \
        ", {
          nextId: nextId,
          limit: limit,
        }).then((rows) => {
          let ids = [], logins = {};
          if (rows)
            for (let row of rows) {
              ids.push(parseInt(row.id));
              logins[row.id] = row.login;
            }
          return {ids: ids, data: logins};
        });
      },
      getListOfIds: () => this.db.manyOrNone("\
        select id \
        from malrec_users \
        where 1=1 \
        " + (onlyNew ? " and reg_date is null " : "") + "\
        order by id asc \
      ", {
      }).then((rows) => {
        return !rows ? [] : (rows.map((row) => row.id));
      }),
      totalIdsCnt: () => this.db.one("\
        select count(*) as cnt \
        from malrec_users \
        where 1=1 \
        " + (onlyNew ? " and reg_date is null " : "") + "\
      ").then((row) => row.cnt),
      getDataForIds: (ids) => {
        return this.db.manyOrNone("\
          select id, login \
          from malrec_users \
          where id in (" + ids.join(', ') + ") \
        ").then((rows) => {
          let logins = {};
          if (rows)
            for (let row of rows) {
              logins[row.id] = row.login;
            }
          return logins;
        });
      },
      fetch: (id, login) => this.provider.getProfileInfo({login: login}),
      process: (id, obj, login) => this.processer.processProfile(id, login, obj),
    });
  }

  /**
   *
   */
  grabUserListsUpdated() {
    //todo_later: can add option 'onlyActive' - only users with list_update_ts > some date 
    // (year ago for example)
    //add option 'woList' - rare task to check if users w/o list created new list
    let key = 'UserListsUpdated';
    return this.grabByIds({
      key: key,
      isByListOfIds: false,
      getNextIds: (nextId, limit) => {
        return this.db.manyOrNone("\
          select id, login, list_update_ts \
          from malrec_users \
          where id >= $(nextId) \
            and need_to_check_list = false and list_update_ts is not null \
          order by id asc \
          limit $(limit) \
        ", {
          nextId: nextId,
          limit: limit,
        }).then((rows) => {
          let ids = [], data = {};
          if (rows)
            for (let row of rows) {
              ids.push(parseInt(row.id));
              data[row.id] = {
                login: row.login, 
                listUpdatedTs: row.list_update_ts, 
              };
            }
          return {ids: ids, data: data};
        });
      },
      getListOfIds: () => this.db.manyOrNone("\
        select id \
        from malrec_users \
        where id >= $(nextId) \
          and need_to_check_list = false and list_update_ts is not null \
        order by id asc \
      ", {
      }).then((rows) => {
        let ids = !rows ? [] : rows.map((row) => parseInt(row.id));
        if (onlyNew)
          ids = (ids);
        return ids;
      }),
      totalIdsCnt: () => this.db.one("\
        select count(*) as cnt \
        from malrec_users \
        where 1=1 \
          and need_to_check_list = false and list_update_ts is not null \
      ").then((row) => row.cnt),
      getDataForIds: (ids) => {
        return this.db.manyOrNone("\
          select id, login, list_update_ts \
          from malrec_users \
          where id in (" + ids.join(', ') + ") \
        ").then((rows) => {
          let data = {};
          if (rows)
            for (let row of rows) {
              data[row.id] = {
                login: row.login, 
                listUpdatedTs: row.list_update_ts, 
              };
            }
          return data;
        });
      },
      fetch: (id, data) => {
        return this.provider.getLastUserListUpdates({login: data.login});
      },
      process: (id, updatedDate, data) => {
        return this.processer.processUserListUpdated(id, data.login, 
          data.listUpdatedTs, updatedDate);
      },
    });
  }

  /**
   * onlyNew - only for users with never checked yet list
   * onlyNeed - only with flag need_to_check_list == true
   */
  grabUserLists(onlyNew = true, onlyNeed = false) {
    let key = (onlyNeed ? 'UpdatedUserLists' : (onlyNew ? 'NewUserLists' : 'AllUserLists'));
    return this.grabByIds({
      key: key,
      isByListOfIds: false,
      getNextIds: (nextId, limit) => {
        return this.db.manyOrNone("\
          select id, login, list_update_ts, list_check_ts, list_id \
          from malrec_users \
          where id >= $(nextId) \
          " + (onlyNew ? " and list_check_ts is null " : "") + "\
          " + (onlyNeed ? " and need_to_check_list = true" : "") + "\
          order by id asc \
          limit $(limit) \
        ", {
          nextId: nextId,
          limit: limit,
        }).then((rows) => {
          let ids = [], data = {};
          if (rows)
            for (let row of rows) {
              ids.push(parseInt(row.id));
              data[row.id] = {
                login: row.login, 
                listUpdatedTs: row.list_update_ts, 
                listCheckedTs: row.list_check_ts, 
                listId: row.list_id,
              };
            }
          return {ids: ids, data: data};
        });
      },
      getListOfIds: () => this.db.manyOrNone("\
        select id \
        from malrec_users \
        where 1=1 \
        " + (onlyNew ? " and list_check_ts is null " : "") + "\
        " + (onlyNeed ? " and need_to_check_list = true" : "") + "\
        order by id asc \
      ", {
      }).then((rows) => {
        let ids = !rows ? [] : rows.map((row) => parseInt(row.id));
        if (onlyNew)
          ids = (ids);
        return ids;
      }),
      totalIdsCnt: () => this.db.one("\
        select count(*) as cnt \
        from malrec_users \
        where 1=1 \
        " + (onlyNew ? " and list_check_ts is null " : "") + "\
        " + (onlyNeed ? " and need_to_check_list = true" : "") + "\
      ").then((row) => row.cnt),
      getDataForIds: (ids) => {
        return this.db.manyOrNone("\
          select id, login, list_update_ts, list_check_ts, list_id \
          from malrec_users \
          where id in (" + ids.join(', ') + ") \
        ").then((rows) => {
          let data = {};
          if (rows)
            for (let row of rows) {
              data[row.id] = {
                login: row.login, 
                listUpdatedTs: row.list_update_ts, 
                listCheckedTs: row.list_check_ts, 
                listId: row.list_id,
              };
            }
          return data;
        });
      },
      fetch: (id, data) => {
        let getListPromise = () => {
          // Get old list
          return Promise.all([
            this.db.manyOrNone("\
              select item_id, rating \
              from malrec_ratings \
              where user_list_id = $(user_list_id) \
            ", {
              user_list_id: data.listId,
            }),
            this.db.one("\
              select unrated_items \
              from malrec_users \
              where id = $(id) \
            ", {
              id: id
            }),
          ]).then(([rows1, row2]) => {
            let oldList = { 
              ratings: {}, 
              unratedAnimeIdsInList: [], 
              listUpdatedTs: data.listUpdatedTs,
              listCheckedTs: data.listCheckedTs,
            };
            if (rows1)
              for (let row of rows1) {
                oldList.ratings[row.item_id] = row.rating;
              }
            oldList.unratedAnimeIdsInList = row2.unrated_items ? row2.unrated_items : [];
            data.oldList = oldList;

            // Get new list
            return this.provider.getUserList({login: data.login});
          });
        };

        if (0 && !onlyNew && !onlyNeed) {
          //tip: sometimes request to get list can take much time or throw http 429, 
          // so variant with getting list update date first can be faster
          return (!data.listCheckedTs ? Promise.resolve(true) 
            : this.provider.getLastUserListUpdates({login: data.login}))
          .then((updatedDate) => {
            if (updatedDate === true || updatedDate > data.listCheckedTs) {
              return getListPromise();
            } else 
              return null;
          });
        } else
          return getListPromise();
      },
      process: (id, newList, data) => {
        return this.processer.processUserList(id, data.login, data.listId, data.oldList, 
            newList);
      },
    });
  }

}
var cls = MalScanner; //for using "cls.A" as like "self::A" inside class

module.exports = MalScanner;

