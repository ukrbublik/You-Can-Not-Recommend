/**
 * MAL data provider
 * Basic abstract class for MalParser and MalApiClient
 * Get useful info about animes (not mangas), user profiles, 
 *  user anime lists (not manga list), users' recommendations anime-to-anime, 
 *  also social info about users: clubs, friends.
 *
 * @author ukrbublik
 */

const deepmerge = require('deepmerge');
const assert = require('assert');
const Helpers = require('../Helpers');
const _ = require('underscore')._;
const http = require('http');
const request = require('request');
const Queue = require('promise-queue');
const FeedRead = require("feed-read");
const ParseXmlString = require('xml2js').parseString;
const MalError = require('./MalError');
const querystring = require('querystring');


/**
 * 
 */
class MalDataProvider {
  /**
   * @return array default options
   */
  static get DefaultOptions() {
    return {
      queueSizeConcurrent: 20,
      parserQueueSizeConcurrent: 10,
      logHttp: true,
      addr: null,
      retryTimeout: [3000, 5000],
      maxRetries: 7,
      type: 'parser',
    }
  }

  constructor() {
  }

  /**
   *
   */
  init(options = {}, id) {
    this.id = id;
    this.options = deepmerge.all([ cls.DefaultOptions, options ]);
    this.queue = new Queue(this.options.type == 'parser' ? this.options.parserQueueSizeConcurrent 
      : this.options.queueSizeConcurrent, 
      Infinity);

    this.cookies = null;
    return this.updateCookies();
  }

  /**
   *
   */
  updateCookies() {
    if (this.options.type == "webProxy" && this.options.webProxyType == 'Glype') {
      return new Promise((resolve, reject) => {
        let j = request.jar();
        request({
          url: this.options.addr, 
          jar: j
        }, (error, response, body) => {
          if (error)
            reject(error);
          else {
            let cookie_string = j.getCookieString(this.options.addr);
            this.cookies = j.getCookies(this.options.addr);
            resolve();
          }
        });
      });
    } else {
      //no need in cookies
      return Promise.resolve();
    }
  }

  /**
   *
   */
  canAddMoreToQueue() {
    //keep waiting queue length as same as max concurrent length (to always have reserve)
    return (this.queue.getQueueLength() < this.queueConcurrentSize() * 1);
  }

  /**
   * max "concurrent" (or "pending") tasks (tasks that are resolving at same time concurrently)
   */
  queueConcurrentSize() {
    return this.queue.maxPendingPromises;
  }

  /**
   *
   */
  setQueueConcurrentSize(queueConcurrentSize) {
    this.queue.maxPendingPromises = queueConcurrentSize;
  }

  //-----------------------------------------------------------

  //Network routines:

  _log(url, err, time) {
    if (!this.options.logHttp)
      return;

    if (!err) {
      console.log("[" + this.id + "] ok {" + time + " ms" + ", " 
        + "q " + this.queue.getPendingLength() + "/" + this.queue.getQueueLength() + "}"
        + " " + url);
    } else {
      console.error("[" + this.id + "] !! {" + time + " ms" + ", " 
        + "q " + this.queue.getPendingLength() + "/" + this.queue.getQueueLength() + "}"
        + " " + url, 
        (err instanceof MalError ? (err.code ? err.code : err.statusCode) : err));
    }
  }

  _logRetry(url, error, retryNo, retryTimeout) {
    if (!this.options.logHttp)
      return;
    console.log("[" + this.id + "] !  {" + (error instanceof MalError ? 
      "HTTP " + error.statusCode : error.code) + "}" 
      + " Retry #" + (retryNo) + " after " + retryTimeout + "ms for url: "
      + url);
  }

  getLoadUrlPromise (url, retry = -1) {
    let t1;
    return new Promise((resolve, reject) => {

      let reqConfig = {};
      if (this.options.type == "webProxy") {
        if (this.options.webProxyType == 'Glype') {
          /*if (0) {
            reqConfig.method = 'GET';
            reqConfig.uri = this.options.addr + '/browse.php' + '?' + querystring.stringify({
              u: url,
              b: 0,
            });
            reqConfig.headers = {
              Referer: this.options.addr,
            }
          } else*/ {
            reqConfig.method = 'POST';
            reqConfig.followAllRedirects = true;
            reqConfig.uri = this.options.addr + '/includes/process.php?action=update';
            reqConfig.form = {
              u: url,
              //encodeURL: 0,
              submit: 'Enter',
            };
            reqConfig.headers = {
              Referer: this.options.addr,
            }
          }
        } else if (this.options.webProxyType == 'PHProxy') {
          reqConfig.method = 'POST';
          reqConfig.followAllRedirects = true;
          reqConfig.uri = this.options.addr + '/index.php';
          reqConfig.form = {
            q: url,
            hl: {
              //include_form: 'on',
              remove_scripts: 'on',
              //accept_cookies: 'on',
              //show_images: 'on',
              //show_referer: 'on',
              //base64_encode: 'on',
              //strip_meta: 'on',
              //session_cookies: 'on',
            }
          };
        } else 
          throw new Error("Unknown web proxy type " + this.options.webProxyType);
      } else {
        reqConfig.method = 'GET';
        reqConfig.uri = url;
        if (this.options.type == "proxy") {
          reqConfig.proxy = this.options.addr;
        }
      }
      if (this.options.type == "proxy" || this.options.type == "webProxy") {
        reqConfig.timeout = 30*1000;
      }

      t1 = new Date();
      if (this.cookies) {
        let j = request.jar();
        for (let cookie of this.cookies)
          j.setCookie(cookie, reqConfig.uri);
        reqConfig.jar = j;
      }

      request(reqConfig, (error, response, body) => {
        if (!error && response.statusCode != 200) {
          error = new MalError(http.STATUS_CODES[response.statusCode], 
            response.statusCode, url);
        }

        if (!error && (!body || !this.isTrueMalSite(url, body))) {
          error = new MalError("Bad body", response.statusCode, url, 3);
        }
        if (error && error.statusCode == 404) {
          if (this.isTrueMalSite(url, body))
            error.errCode = 2;
        }

        let canRetry = false;
        let retryTimeout = 0;
        if (error && (
          //MAL's Too Many Requests
          error instanceof MalError && error.statusCode == 429 
          //Heroku's errors
          || error.code == 'EHOSTUNREACH' || error.code == 'EAI_AGAIN'
          //Heroku's "Service Unavailable" 503
          || error instanceof MalError && error.statusCode == 503
        )) {
          canRetry = ((retry+1) < this.options.maxRetries);
          if (error.statusCode == 503 && this.options.addr !== null) {
            //If Heroku says 503, wait more (30s at least)
            retryTimeout = 30*1000;
          }
        }
        if (canRetry) {
          retry++;
          if(!retryTimeout)
            retryTimeout = parseInt(this.options.retryTimeout[0] + Math.random() 
            * (this.options.retryTimeout[1] - this.options.retryTimeout[0]));
          this._logRetry(url, error, retry, retryTimeout);
          setTimeout(() => {
            this.getLoadUrlPromise(url, retry).then((body) => {
              resolve(body);
            }).catch((err) => {
              reject(err);
            });
          }, retryTimeout);
        } else {
          if (!error) {
            resolve(body);
          } else {
            reject(error);
          }
        }
      });
    }).then((res) => {
      let t2 = new Date();
      if (retry == -1)
        this._log(url, null, t2 - t1);
      return res;
    }).catch((err) => {
      let t2 = new Date();
      if (retry == -1)
        this._log(url, err, t2 - t1);
      throw err;
    });
  }

  /**
   * If using web proxy and getting 404, we must be sure that it's from original MAL site
   */
  isTrueMalSite(url, body) {
    let isHtml = (body.indexOf("<html>") != -1);
    if (isHtml)
      return body.indexOf('MyAnimeList') != -1;
    else
      return true; //proxies should return json/xml as is
  }

  /**
   *
   */
  loadUrl(url, retry = -1) {
    if (retry == -1)
      return this.queue.add(() => this.getLoadUrlPromise(url));
    else
      return this.getLoadUrlPromise(url, retry);
  }

  /**
   *
   */
  headUrl(url) {
    return this.queue.add(() => new Promise((resolve, reject) => {
      request.head({
        url: url
      }, (error, response) => {
        if (!error && response.statusCode != 200) {
          error = new MalError(http.STATUS_CODES[response.statusCode], 
            response.statusCode, url);
        }

        if (!error) {
          resolve(response);
        } else {
          reject(error);
        }
      });
    }));
  }

  /**
   *
   */
  loadJson(url) {
    return this.loadUrl(url)
      .catch((err) => {
        delete err.body;
        delete err.response;
        throw err;
      })
      .then((body) => JSON.parse(body));
  }

  /**
   *
   */
  loadXml(url) {
    return this.loadUrl(url)
      .catch((err) => {
        delete err.body;
        delete err.response;
        throw err;
      })
      .then((body) => {
        return new Promise((resolve, reject) => {
          ParseXmlString(body, (err, res) => {
            if (err)
              reject(err);
            else
              resolve(res);
          });
        });
      });
  }

  /**
   *
   */
  loadRss(url) {
    return this.loadUrl(url)
      .catch((err) => {
        delete err.body;
        delete err.response;
        throw err;
      })
      .then((body) => {
        return new Promise((resolve, reject) => {
          FeedRead.rss(body, (err, items) => {
            if (err)
              reject(err);
            else
              resolve(items);
          });
        });
      });
  }

  /**
   *
   */
  loadHtml(url) {
    return this.loadUrl(url)
      .catch((err) => {
        delete err.body;
        delete err.response;
        throw err;
      })
      .then((body) => {
        let $ = cheerio.load(body);
        return [$, body];
      });
  }

  //-----------------------------------------------------------

  // Data provider interface:


  /**
   *
   */
  setParserQueueConcurrentSize({queueConcurrentSize}) {
    throw new Error("abstract");
  }

  /**
   * @return null/string login or null if no user with such id ("" if can't parse)
   */
  userIdToLogin({userId}) {
    throw new Error("abstract");
  }

  /**
   * @return null/Date
   */
  getLastUserListUpdates({login}) {
    throw new Error("abstract");
  }

  /**
   * @return null/object { id: <id>, login: <string>,  joinedDate: <Date>, gender: <Male/Female>, 
   * favs: [<animeId>, ...], friendsLogins: [<userLogin>, ...], clubsIds: [<clubId>, ...] }
   */
  getProfileInfo({login}) {
    throw new Error("abstract");
  }

  /**
   * @return object { friendsLogins: [<userLogin>, ...], clubsIds: [<clubId>, ...] }
   */
  getUserSocialInfo({login}) {
    throw new Error("abstract");
  }

  /**
   * @return null/{ ratings: {<animeId> => <rating>, ..}, listUpdated: <Date>, 
   *  unratedAnimeIdsInList: [<animeId>, ...] }
   */
  getUserList({login, altJson = false}) {
    throw new Error("abstract");
  }

  /**
   * @return int
   */
  getApproxMaxAnimeId({seasonUrl = null}) {
    throw new Error("abstract");
  }

  /**
   * @return object { <id>: <name> }
   */
  getGenres() {
    throw new Error("abstract");
  }

  /**
   * @return null/object { id: <id>, name: <string>, type: <typeName>, genres: [<genreId>, ...], 
   *  rels: { <animeId> => <relName> }, recs: { <animeId> => <weight> } }
   *  where relName = Other, Prequel, Sequel, Side story, Parent story, Alternative version, 
   *   Spin-off, Summary, ...
   *  typeName = Special, OVA, Movie, TV, ONA
   */
  getAnimeInfo({animeId}) {
    throw new Error("abstract");
  }

  /**
   * @return null/object { <animeId> => <weight> }
   */
  getAnimeUserrecs({animeId}) {
    throw new Error("abstract");
  }

  /**
   * @return object clubs { <clubId> => {membersCnt: <membersCnt>} }
   */
  scanClubs() {
    throw new Error("abstract");
  }

  /**
   * @return null/object { name: <string>, type: <clubType>, animeIds: [<animeId>, ...], 
   *  membersLogins: [<userLogin>, ...] }
   *  where clubType = Other, Games, Cities & Neighborhoods, ...?
   */
  getClubInfo({clubId}) {
    throw new Error("abstract");
  }

}
var cls = MalDataProvider; //for using "cls.A" as like "self::A" inside class

module.exports = MalDataProvider;
