/**
 * MAL data processer
 * Processes results from MAL data provider and saves to DB.
 *
 * @author ukrbublik
 */

const deepmerge = require('deepmerge');
const assert = require('assert');
const Helpers = require('../Helpers');
const _ = require('underscore')._;
const MalError = require('./MalError');
const pgEscape = require('pg-escape');
const fs = require('fs');


/**
 * 
 */
class MalDataProcesser {
  /**
   * @return array default options
   */
  static get DefaultOptions() {
    return {
      maxRating: 10,
    }
  }


  constructor() {
  }

  /**
   *
   */
  init(options = {}, dbConnection) {
    this.options = deepmerge.all([ cls.DefaultOptions, options ]);
    this.db = dbConnection;

    //get enum values from db
    return Promise.all([
      this.db.manyOrNone("SELECT unnest(enum_range(NULL::malrec_item_type)) as v"),
      this.db.manyOrNone("SELECT unnest(enum_range(NULL::malrec_items_rel)) as v"),
    ]).then(([res1, res2]) => {
      this.itemTypes = [];
      if (res1)
        for (let row of res1)
          this.itemTypes.push(row.v);
      this.itemsRels = [];
      if (res2)
        for (let row of res2)
          this.itemsRels.push(row.v);
    });
  }

  /**
   *
   */
  isSafeToModilfyRatings() {
    //todo_later: true if not training (keep bool 'isTrainig?' in redis)
    return false;
  }


  /**
   *
   */
  processGenres(genres) {
    let promises = [];
    for (let id in genres) {
      promises.push(this.db.none("\
        insert into malrec_genres(id, name) \
        values ($(id), $(name)) \
        on conflict do nothing \
      ", {
        id: id,
        name: genres[id],
      }));
    }
    return Promise.all(promises);
  }


  /**
   *
   */
  processAnime(animeId, anime) {
    if (anime === null)
      return Promise.resolve();

    let promises = [];
    anime.id = animeId;

    //add new anime type enum values
    if (anime.type && this.itemTypes.indexOf(anime.type) == -1) {
      promises.push(
        this.db.query("alter type malrec_item_type add value $(v)", { v: anime.type })
        .then(() => {
          this.itemTypes.push(anime.type);
        }));
    }
    if (Object.keys(anime.rels).length) {
      let relAnimesIds = Object.keys(anime.rels).map(Number);

      //add new relation type enum values
      let newRels = _.difference(relAnimesIds.map((fromId) => anime.rels[fromId]).filter((v) => v), 
        this.itemsRels);
      for (let relName of newRels) {
        promises.push(
          this.db.query("alter type malrec_items_rel add value $(v)", { v: relName })
          .then(() => {
            this.itemsRels.push(relName);
          }));
      }
      
      //if there is already franchise_id in related animes, get it; otherwise create new one.
      //if there are no some related animes in db yet, create empty ones
      promises.push(
        this.db.tx((t) => {
          return t.manyOrNone("\
            select id, franchise_id \
            from malrec_items \
            where id in(" + relAnimesIds.join(", ") + ")"
          ).then((rows) => {
            let rowsWithFr = !rows ? [] : rows.filter(row => row.franchise_id);
            let promise;
            if (rowsWithFr.length) {
              let frId = rowsWithFr[0].franchise_id;
              if (rowsWithFr.filter(row => row.franchise_id != frId).length > 0)
                console.warn("Multi-franchise relations for " + animeId);
              promise = Promise.resolve(frId);
            } else {
              promise = t.one("\
                select nextval('malrec_items_franchise_id_seq'::regclass) as fr_id \
              ").then((row) => row.fr_id);
            }
            return promise.then((frId) => {
              anime.franchise_id = frId;
              let idsWoFr = !rows ? [] : rows.filter(row => !row.franchise_id).map(row => row.id);
              if (!idsWoFr.length)
                promise = Promise.resolve();
              else
                promise = t.query("\
                  update malrec_items \
                  set franchise_id = $(frId) \
                  where id in (" + idsWoFr.join(', ') + ")", {
                    frId: frId
                  });
              return promise.then(() => {
                let alrIds = !rows ? [] : rows.map(row => row.id);
                let idsToIns = _.difference(relAnimesIds, alrIds);
                if (idsToIns.length)
                  promise = Promise.resolve();
                else {
                  let iv = "";
                  for (let id of idsToIns)
                    iv += (iv ? ", " : "") + "(" + id + ", " + frId + ")";
                  promise = t.query("\
                    insert into malrec_items(id, franchise_id) \
                    values " + iv);
                }
                return promise;
              });
            });
          });
        })
      );
    }
    return Promise.all(promises).then(() => {
      return Promise.all([
        this.db.oneOrNone("\
          select * \
          from malrec_items \
          where id = $(id) \
        ", {
          id: animeId,
        }),
        this.db.manyOrNone("\
          select to_id, rel \
          from malrec_items_rels \
          where from_id = $(id) \
        ", {
          id: animeId,
        })
      ]).then(([row1, rows2]) => {
        let oldAnime = row1;
        let oldRels = {};
        if (rows2)
          for (let row of rows2) {
            oldRels[row.to_id] = row.rel;
          }
        let newRels = anime.rels;
        let oldRelsIds = Object.keys(oldRels).map(Number);
        let newRelsIds = Object.keys(newRels).map(Number);

        //ins/upd anime
        let cols = ['id', 'name', 'type', 'genres', 'franchise_id'];
        let newAnime = _.pick(anime, cols);
        let promises = [];
        let params = {};
        let sql = "";
        if (!oldAnime) {
          cols = Object.keys(newAnime);
          params = newAnime;
          let vals = cols.map(c => '$('+c+')'+(c == 'genres' ? '::integer[]' : ''));
          sql = "insert into malrec_items(" + cols.join(", ") + ")" 
            + " values(" + vals.join(", ") + ")"
            + " on conflict do nothing";
        } else {
          cols = cols.filter((k) => (k == 'genres' 
            ? !Helpers.isSameElementsInArrays(newAnime[k], oldAnime[k]) 
            : newAnime[k] != oldAnime[k]));

          if (cols.length) {
            params = _.pick(newAnime, cols);
            params.id = newAnime.id;
            sql = "update malrec_items" 
              + " set " + cols.map(c => c+'='+'$('+c+')').join(", ")
              + " where id = $(id)";
          }
        }
        if (sql != '')
          promises.push(this.db.query(sql, params));

        //add/del/upd rels
        let relsToUpd = _.pick(newRels, _.intersection(oldRelsIds, newRelsIds)
          .filter(id => oldRels[id] != newRels[id]));
        let relsIdsToDel = _.difference(oldRelsIds, newRelsIds);
        let relsToAdd = _.pick(newRels, _.difference(newRelsIds, oldRelsIds));
        if (relsIdsToDel.length || Object.keys(relsToAdd).length 
          || Object.keys(relsToUpd).length) {
          promises.push(this.db.tx((t) => {
            let batch = [];
            if (relsIdsToDel.length) {
              batch.push(t.query("\
                delete from malrec_items_rels \
                where from_id = $(from_id) and to_id in(" + relsIdsToDel.join(", ") + ")", {
                  from_id: animeId,
                }));
            }
            if (Object.keys(relsToAdd).length) {
              let iv = "";
              let params = {};
              params.from_id = animeId;
              for (let id in relsToAdd) {
                params['rel_'+id] = relsToAdd[id];
                iv += (iv ? ", " : "") + "($(from_id), " + id + ", $(rel_" + id + "))";
              }
              batch.push(t.query("\
                insert into malrec_items_rels(from_id, to_id, rel) \
                values " + iv + "\
                on conflict do nothing", 
              params));
            }
            if (Object.keys(relsToUpd).length) {
              for (let id in relsToUpd) {
                batch.push(t.query("\
                  update malrec_items_rels \
                  set rel = $(rel) \
                  where from_id = $(from_id) and to_id = $(to_id)", {
                    rel: relsToUpd[id],
                    from_id: animeId,
                    to_id: id,
                  }));
              }
            }
            return t.batch(batch);
          }));
        }
        return Promise.all(promises);
      });
    });
  }


  /**
   *
   */
  processAnimeUserrecs(animeId, newRecs) {
    if (newRecs === null)
      return Promise.resolve();

    return this.db.manyOrNone("\
      select to_id, weight \
      from malrec_items_recs \
      where from_id = $(id) \
    ", {
      id: animeId,
    }).then((rows) => {
      let oldRecs = {};
      if (rows)
        for (let row of rows) {
          oldRecs[row.to_id] = row.weight;
        }
      let oldRecsIds = Object.keys(oldRecs).map(Number);
      let newRecsIds = Object.keys(newRecs).map(Number);

      //check new anime ids in recs for existence in db
      let promise;
      let newAnimeIds = _.difference(newRecsIds, oldRecsIds);
      if (!newAnimeIds.length)
        promise = Promise.resolve();
      else
        promise = this.db.manyOrNone("\
          select id \
          from malrec_items \
          where id in(" + newAnimeIds.join(", ") + ", " + animeId + ")"
        ).then((rows) => {
          let alrAnimeIds = !rows ? [] : rows.map(row => row.id);
          let animeIdsToIns = _.difference(newAnimeIds, alrAnimeIds);
          if (animeIdsToIns.length) {
            let iv = "";
            for (let id of animeIdsToIns)
              iv += (iv ? ", " : "") + "(" + id + ")";
            return this.db.query("insert into malrec_items(id)"
             + " values " + iv 
             + " on conflict do nothing");
          }
        });
      return promise.then(() => {
        //add/del/upd recs
        let recsToUpd = _.pick(newRecs, _.intersection(oldRecsIds, newRecsIds)
          .filter(id => oldRecs[id] != newRecs[id]));
        let recsIdsToDel = _.difference(oldRecsIds, newRecsIds);
        let recsToAdd = _.pick(newRecs, _.difference(newRecsIds, oldRecsIds));
        if (recsIdsToDel.length || Object.keys(recsToAdd).length 
          || Object.keys(recsToUpd).length) {
          return this.db.tx((t) => {
            let batch = [];
            if (recsIdsToDel.length) {
              batch.push(t.query("\
                delete from malrec_items_recs \
                where from_id = $(from_id) and to_id in(" + recsIdsToDel.join(", ") + ")", {
                  from_id: animeId,
                }));
            }
            if (Object.keys(recsToAdd).length) {
              let iv = "";
              let params = {};
              params.from_id = animeId;
              for (let id in recsToAdd) {
                params['rel_'+id] = recsToAdd[id];
                iv += (iv ? ", " : "") + "($(from_id), " + id + ", $(rel_" + id + "))";
              }
              batch.push(t.query("\
                insert into malrec_items_recs(from_id, to_id, weight) \
                values " + iv + "\
                on conflict do nothing",
              params));
            }
            if (Object.keys(recsToUpd).length) {
              for (let id in recsToUpd) {
                batch.push(t.query("\
                  update malrec_items_recs \
                  set weight = $(weight) \
                  where from_id = $(from_id) and to_id = $(to_id)", {
                    weight: recsToUpd[id],
                    from_id: animeId,
                    to_id: id,
                  }));
              }
            }
            batch.push(t.query("\
              update malrec_items \
              set recs_update_ts = now(), recs_check_ts = now() \
              where id = $(id) \
            ", {
              id: animeId,
            }));
            return t.batch(batch);
          });
        } else {
          return this.db.query("\
            update malrec_items \
            set recs_check_ts = now() \
            where id = $(id) \
          ", {
            id: animeId,
          });
        }
      });
    });
  }


  /**
   *
   */
  processUserIdToLogin(userId, userLogin) {
    if (userLogin === null)
      return Promise.resolve();

    assert(userId > 0);
    //assert(typeof userLogin == 'string' && userLogin.length > 0);

    return this.db.query("\
      insert into malrec_users(id, login) \
      values ($(id), $(login)) \
      on conflict do nothing \
    ", {
      id: userId,
      login: userLogin,
    });
  }


  /**
   *
   */
  processProfile(userId, userLogin, user) {
    if (user === null)
      return Promise.resolve();

    return this.db.oneOrNone("\
      select * \
      from malrec_users \
      where login = $(login) \
    ", {
      login: userLogin,
    }).then((oldUser) => {
      let newUser = {
        id: user.id,
        login: user.login,
        gender: user.gender,
        reg_date: user.joinedDate,
        fav_items: user.favs,
      };
      let vals = {}, cols = Object.keys(newUser);
      let sql = "";
      if (!oldUser) {
        vals = newUser;
        sql = "insert into malrec_users(" + cols.join(", ") + ")" 
          + " values(" + cols.map((k) => '$('+k+')').join(", ") + ")"
          + " on conflict do nothing";
      } else {
        assert(newUser.login == oldUser.login);
        cols = cols.filter((k) => (k == 'fav_items' 
          ? !Helpers.isSameElementsInArrays(newUser[k], oldUser[k])
          : newUser[k] != oldUser[k]));

        if (cols.length > 0) {
          vals = _.pick(newUser, cols);
          vals.login = newUser.login;
          sql = "update malrec_users" 
            + " set " + cols.map((k) => k+'='+'$('+k+')').join(", ")
            + " where login = $(login)";
        }
      }
      return (sql != '' ? this.db.query(sql, vals) : Promise.resolve()).then(() => {
        /*if (cols.indexOf('fav_items') != -1)
          //update most_rated_items
          return this.db.func("malrec_update_user_most_rated_items", 
            [userId, this.options.maxRating]);*/
      });
    });
  }

  /**
   *
   */
  processUserListUpdated(userId, userLogin, listUpdatedTs, newUpdatedDate) {
    if (newUpdatedDate === null)
      return Promise.resolve();
    
    if (newUpdatedDate > listUpdatedTs) {
      return this.db.query("\
        update malrec_users \
        set need_to_check_list = true \
        where id = $(id) \
      ", {
        id: userId,
      });
    } else return Promise.resolve();
  }

  /**
   *
   */
  processUserList(userId, userLogin, listId, oldList, newList) {
    let hadList = oldList && Object.keys(oldList.ratings).length > 0;
    let hasList = newList && Object.keys(newList.ratings).length > 0;
    let oldListAnimeIds = oldList ? Object.keys(oldList.ratings).map(Number) : [];
    let newListAnimeIds = newList ? Object.keys(newList.ratings).map(Number) : [];

    let promise;
    if (!listId && hasList) {
      promise = this.db.tx((t) => {
        return t.query("\
          update malrec_users \
          set list_id = nextval('malrec_users_list_id_seq'::regclass) \
          where id = $(id) and list_id is null \
        ", {
          id: userId,
        }).then(() => t.one("\
          select list_id \
          from malrec_users \
          where id = $(id) \
        ", {
          id: userId,
        })).then((row) => {
          listId = row.list_id;
          return listId;
        });
      });
    } else {
      promise = Promise.resolve(listId);
    }
    return promise.then(() => {
      if (!hasList) {
        return this.db.query("\
          update malrec_users \
          set list_check_ts = now(), need_to_check_list = false \
          where id = $(id) \
        ", {
          id: userId,
        });
      } else {
        assert(!!listId);
        let unratedListChanged = !Helpers.isSameElementsInArrays(
          oldList.unratedAnimeIdsInList, newList.unratedAnimeIdsInList);
        //add/del/upd ratings
        let ratsToUpd = _.pick(newList.ratings, _.intersection(oldListAnimeIds, newListAnimeIds)
          .filter(id => oldList.ratings[id] != newList.ratings[id]));
        let ratsIdsToDel = _.difference(oldListAnimeIds, newListAnimeIds);
        let ratsToAdd = _.pick(newList.ratings, _.difference(newListAnimeIds, oldListAnimeIds));
        let affectedAnimeIds = Object.keys(ratsToAdd).map(Number)
          .concat(Object.keys(ratsToUpd).map(Number), ratsIdsToDel);
        if (unratedListChanged || ratsIdsToDel.length || Object.keys(ratsToAdd).length
         || Object.keys(ratsToUpd).length) {
          //check new anime ids in list for existence in db
          let promise;
          let newAnimeIds = _.difference(newListAnimeIds, oldListAnimeIds).map(Number);
          if (!newAnimeIds.length)
            promise = Promise.resolve();
          else
            promise = this.db.manyOrNone("\
              select id \
              from malrec_items \
              where id in(" + newAnimeIds.join(", ") + ")"
            ).then((rows) => {
              let alrAnimeIds = !rows ? [] : rows.map(row => parseInt(row.id));
              let animeIdsToIns = _.difference(newAnimeIds, alrAnimeIds);
              if (animeIdsToIns.length) {
                let iv = "";
                for (let id of animeIdsToIns)
                  iv += (iv ? ", " : "") + "(" + id + ")";
                return this.db.query("insert into malrec_items(id)"
                  + " values " + iv
                  + " on conflict do nothing");
              }
            });
          return promise.then(() => this.db.tx((t) => {
            let batch = [];
            if (unratedListChanged) {
              batch.push(t.query("\
                update malrec_users \
                set unrated_items = $(unrated_items) \
                where id = $(id) \
              ", {
                id: userId,
                unrated_items: newList.unratedAnimeIdsInList,
              }));
            }
            if (ratsIdsToDel.length) {
              batch.push(t.query("\
                update malrec_ratings \
                set " + (this.isSafeToModilfyRatings() ? "rating" : "new_rating") + " = 0 \
                where user_list_id = $(listId) and item_id in(" + ratsIdsToDel.join(", ") + ")", {
                  listId: listId,
                }));
            }
            if (Object.keys(ratsToAdd).length) {
              let iv = "";
              let params = {};
              params.listId = listId;
              for (let id in ratsToAdd) {
                params['rat_'+id] = ratsToAdd[id];
                iv += (iv ? ", " : "") + "($(listId), " + id + ", $(rat_" + id + "), " 
                  + (this.isSafeToModilfyRatings() ? "false" : "true") + ")";
              }
              batch.push(t.query("\
                insert into malrec_ratings(user_list_id, item_id, rating, is_new) \
                values " + iv, params));
            }
            if (Object.keys(ratsToUpd).length) {
              for (let id in ratsToUpd) {
                batch.push(t.query("\
                  update malrec_ratings \
                  set " + (!this.isSafeToModilfyRatings() ? "rating" : "new_rating") + " = $(rating) \
                  where user_list_id = $(listId) and item_id = $(item_id)", {
                    rating: ratsToUpd[id],
                    listId: listId,
                    item_id: id,
                  }));
              }
            }
            if (ratsIdsToDel.length || Object.keys(ratsToAdd).length 
              || Object.keys(ratsToUpd).length) {
              batch.push(t.query("\
                update malrec_users \
                set list_update_ts = $(list_update_ts), is_modified = true \
                where id = $(id) \
              ", {
                id: userId,
                list_update_ts: newList.listUpdatedTs,
              }));

              batch.push(t.func("malrec_update_user_most_rated_items", 
                [userId, this.options.maxRating]));

              if (affectedAnimeIds) {
                batch.push(t.query("\
                  update malrec_items \
                  set is_modified = true \
                  where id in(" + affectedAnimeIds.join(", ") + ") and is_modified = false \
                "));
              }
            }
            batch.push(t.query("\
              update malrec_users \
              set list_check_ts = now(), need_to_check_list = false \
              where id = $(id) \
            ", {
              id: userId,
            }));
            return t.batch(batch);
          }));
        } else {
          return this.db.query("\
            update malrec_users \
            set list_check_ts = now(), need_to_check_list = false \
            where id = $(id) \
          ", {
            id: userId,
          });
        }
      }
    });
  }

}
var cls = MalDataProcesser; //for using "cls.A" as like "self::A" inside class

module.exports = MalDataProcesser;

