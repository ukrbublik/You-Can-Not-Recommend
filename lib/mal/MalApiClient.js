/**
 * MAL API (unofficial) client
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
const querystring = require('querystring');
const MalDataProvider = require('./MalDataProvider');
const MalError = require('./MalError');


/**
 * 
 */
class MalApiClient extends MalDataProvider {
  /**
   * @return array default options
   */
  static get DefaultOptions() {
    return _.extend(super.DefaultOptions, {
      type: 'apiClient',
    });
  }

  constructor() {
    super();
  }

  /**
   *
   */
  init(options = {}, id) {
    return super.init(options, id);
  }

  /**
   *
   */
  queryMethod(name, params) {
    let url = this.options.addr + '/' + name + (Object.keys(params).length ? 
      '?' + querystring.stringify(params) : '');
    return this.loadJson(url).then((result) => {
      if (result.err) {
        throw MalError.fromJSON(result.err);
      } else if (result.res !== undefined)
        return result.res;
      else {
        throw new MalError("Malformed JSON", 200, url, 3);
      }
    }).catch((err) => {
      throw err;
    });
  }

  //-----------------------------------------------------------

  setParserQueueConcurrentSize({queueConcurrentSize}) {
    return this.queryMethod('setParserQueueConcurrentSize', 
      {queueConcurrentSize: queueConcurrentSize});
  }

  userIdToLogin({userId}) {
    return this.queryMethod('userIdToLogin', {userId: userId});
  }

  getLastUserListUpdates({login}) {
    return this.queryMethod('getLastUserListUpdates', {login: login}).then((date) => {
      if (date)
        date = new Date(date);
      return date;
    });
  }

  getProfileInfo({login}) {
    return this.queryMethod('getProfileInfo', {login: login}).then((user) => {
      if (user && user.joinedDate)
        user.joinedDate = new Date(user.joinedDate);
      return user;
    });
  }

  getUserSocialInfo({login}) {
    return this.queryMethod('getUserSocialInfo', {login: login});
  }

  getUserList({login, altJson = false}) {
    return this.queryMethod('getUserList', {login: login, altJson: altJson});
  }

  getApproxMaxAnimeId({seasonUrl = null}) {
    return this.queryMethod('getApproxMaxAnimeId', {seasonUrl: seasonUrl});
  }

  getGenres() {
    return this.queryMethod('getGenres', {});
  }

  getAnimeInfo({animeId}) {
    return this.queryMethod('getAnimeInfo', {animeId: animeId});
  }

  getAnimeUserrecs({animeId}) {
    return this.queryMethod('getAnimeUserrecs', {animeId: animeId});
  }

  scanClubs() {
    return this.queryMethod('scanClubs', {});
  }

  getClubInfo({clubId}) {
    return this.queryMethod('getClubInfo', {clubId: clubId});
  }

}
var cls = MalApiClient; //for using "cls.A" as like "self::A" inside class

module.exports = MalApiClient;

