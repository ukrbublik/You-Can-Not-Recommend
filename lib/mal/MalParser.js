/**
 * MAL parser
 *
 * @author ukrbublik
 */

const deepmerge = require('deepmerge');
const assert = require('assert');
const Helpers = require('../Helpers');
const _ = require('underscore')._;
const http = require('http');
const request = require('request');
const cheerio = require('cheerio');
const Queue = require('promise-queue');
const querystring = require('querystring');
const MalDataProvider = require('./MalDataProvider');
const MalError = require('./MalError');


/**
 * 
 */
class MalParser extends MalDataProvider {
  /**
   * @return array default options
   */
  static get DefaultOptions() {
    return _.extend(super.DefaultOptions, {
      type: 'parser',
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
  loadUrl(url) {
    return super.loadUrl(url);
  }

  /**
   *
   */
  loadHtml(url) {
    return this.loadUrl(url)
      .catch((err) => {
        if (err instanceof MalError && err.body) {
          let $ = cheerio.load(err.body);
          if ($('#content .badresult').length) {
            err.errCode = 1;
            err.errMessage = $('#content .badresult').text();
          } else if ($('#content .error404').length) {
            err.errCode = 2;
          }
        }
        throw err;
      })
      .then((body) => {
        let $ = cheerio.load(body);
        return [$, body];
      })
      .then(([$, body]) => {
        //check response to not have error strings
        let err = null;
        if ($('#content .badresult').length) {
          err = new MalError($('#content .badresult').text(), 200, url, 1);
        }
        if (err !== null)
          throw err;

        return $;
      });
  }

  //-----------------------------------------------------------

  setParserQueueConcurrentSize({queueConcurrentSize}) {
    //console.log("Set parser queue concurrent size = " + queueConcurrentSize);
    this.setQueueConcurrentSize(queueConcurrentSize);
    return Promise.resolve(queueConcurrentSize);
  }

  userIdToLogin({userId}) {
    let url = "https://myanimelist.net/comments.php?id=" + userId;
    return this.loadHtml(url).catch((err) => {
      if (err instanceof MalError && err.errCode == 2) {
        return null; // No MAL user with such id!
      } else throw err;
    }).then($ => {
      let login = "";
      let match = $('#contentWrapper h1').text().match(/(.+)'s Comments/);
      if (match !== null)
        login = match[1];
      return login;
    });
  }


  getLastUserListUpdates({login}) {
    let url = "https://myanimelist.net/rss.php?type=rw&u=" + login;
    return this.loadRss(url).then((items) => {
      let listUpdateDate = items && items.length ? items[0].published : null;
      return listUpdateDate;
    });
  }

  getProfileInfo({login}) {
    let limit, page, url, offset;
    url = "https://myanimelist.net/profile/" + login;
    // or "https://myanimelist.net/profile.php?username=" + login
    return this.loadHtml(url).catch((err) => {
      if (err instanceof MalError && err.errCode == 2) {
        return null; // No MAL user with such id!
      } else throw err;
    }).then(($) => {
      let user = {};
      user.login = login;

      // Parse user id
      let avatarUrl = $('.user-image img[src*="images/userimages/"]').attr('src');
      if (avatarUrl) {
        let match = avatarUrl.match(/images\/userimages\/(\d+)\.\w+/);
        if (match)
          user.id = parseInt(match[1]);
      }
      if (!user.id) {
        //alt
        let blogUrl = $('.user-profile a[href*="rss.php?type=blog&id="]').attr('href');
        if (blogUrl) {
          let match = blogUrl.match(/&id=(\d+)/);
          if (match)
            user.id = parseInt(match[1]);
        }
      }

      // Parse gender, joined date
      let genderStr = $('.user-status-title:contains("Gender")').next('.user-status-data').text();
      if (genderStr)
        user.gender = genderStr; //Male, Female
      let joinedStr = $('.user-status-title:contains("Joined")').next('.user-status-data').text();
      if (joinedStr)
        user.joinedDate = new Date(joinedStr);

      // Parse fav animes
      user.favs = [];
      let favAnimeLinks = $('.user-favorites .favorites-list.anime a.image[href*="/anime/"]');
      if (favAnimeLinks.length) {
        favAnimeLinks.each((ind, a) => {
          let match = $(a).attr('href').match(/anime\/(\d+)\/([^?#/]+)/);
          if (match) {
            user.favs.push( parseInt(match[1]) );
          }
        });
      }

      // Parse cnts
      let clubsCntText = $('.user-profile a[href*="/profile/' + login 
        + '/clubs"] .user-status-data').text();
      if (clubsCntText != "")
        user.clubsCnt = parseInt(clubsCntText);
      let reviewsCntText = $('.user-profile a[href*="/profile/' + login 
        + '/reviews"] .user-status-data').text();
      if (reviewsCntText != "")
        user.reviewsCnt = parseInt(reviewsCntText);
      let recsCntText = $('.user-profile a[href*="/profile/' + login 
        + '/recommendations"] .user-status-data').text();
      if (recsCntText != "")
        user.recsCnt = parseInt(recsCntText);
      let friendsCnt = $('.user-profile a[href*="/profile/' + login + '/friends"]').text();
      if (friendsCnt != "") {
        let match = friendsCnt.match(/All \((\d+)\)/);
        if (match)
          user.friendsCnt = parseInt(match[1]);
      }

      // Load friends
      let getFriendsPromises = [];
      if (0) {
        user.friendsLogins = [];
        if (user.friendsCnt > 0) {
          let links = $('.user-profile .user-friends a[href*="/profile/"]'); //limit 10
          if (links.length == user.friendsCnt) {
            links.each((ind, a) => {
              let match = a.match(/\/profile\/([^?#/]+)/);
              if (match)
                user.friendsLogins.push(match[1]);
            });
          } else {
            limit = 100;
            for (page = 0 ; page < Math.ceil(user.friendsCnt / limit) ; page++) {
              offset = page * limit;
              url = "https://myanimelist.net/profile/" + login + "/friends" + "?offset=" + offset;
              getFriendsPromises.push(this.loadHtml(url).then(($) => {
                let links = $('.majorPad .friendIcon a[href*="/profile/"]');
                links.each((ind, a) => {
                  let match = $(a).attr('href').match(/\/profile\/([^?#/]+)/);
                  if (match)
                    user.friendsLogins.push(match[1]);
                });
              }));
            }
          }
        }
      }
      let getFriendsPromise = Promise.all(getFriendsPromises);

      // Load clubs
      let getClubsPromise;
      if (0) {
        user.clubsIds = [];
        if (user.clubsCnt === undefined || user.clubsCnt > 0) {
          //no limit
          url = "https://myanimelist.net/profile/" + login + "/clubs";
          getClubsPromise = this.loadHtml(url).then(($) => {
            let links = $('#content li a[href*="/clubs.php?cid="]');
            links.each((ind, a) => {
              let match = $(a).attr('href').match(/clubs.php\?cid=(\d+)/);
              if (match)
                user.clubsIds.push(parseInt(match[1]));
            });
          });
        } else {
          getClubsPromise = Promise.resolve();
        }
      } else getClubsPromise = Promise.resolve();

      // Load reviews
      let getReviewsPromises = [];
      if (0) {
        user.reviewedAnimeIds = {};
        if (user.reviewsCnt > 0) { //reviewsCnt - anime + manga reviews
          limit = 10;
          for (page = 0 ; page < Math.ceil(user.reviewsCnt / limit) ; page++) {
            url = "https://myanimelist.net/profile/" + login + "/reviews" + "?p=" + (page+1);
            getReviewsPromises.push(this.loadHtml(url).then(($) => {
              let links = $('#content .borderDark a[href*="/anime/"].hoverinfo_trigger');
              let weights = $('#content .borderDark:has(a[href*="/anime/"]) .lightLink span');
              assert(links.length == weights.length);
              for (let i = 0 ; i < links.length ; i++) {
                let match = $(links[i]).attr('href').match(/anime\/(\d+)\/([^?#/]+)/);
                if (match) {
                  let titleId = parseInt(match[1]);
                  let weight = 0;
                  match = $(weights[i]).text();
                  if (match)
                    weight = parseInt(match);
                    user.reviewedAnimeIds[titleId] = weight;
                }
              }
            }));
          }          
        }
      }
      let getReviewsPromise = Promise.all(getReviewsPromises);

      // Load recommendations
      let getRecsPromise;
      if (0) {
        user.recs = {};
        if (user.recsCnt > 0) {
          //no limit
          url = "https://myanimelist.net/profile/" + login + "/recommendations";
          getRecsPromise = this.loadHtml(url).then(($) => {
            let links = $('#content .picSurround a.hoverinfo_trigger[href*="/anime/"]');
            assert(links.length % 2 == 0);
            for (let i = 0 ; i < links.length ; i+=2) {
              let fromUrl = $(links[i*2]).attr('href');
              let toUrl = $(links[i*2+1]).attr('href');
              let fromMatch = fromUrl.match(/anime\/(\d+)\/([^?#/]+)/);
              let toMatch = toUrl.match(/anime\/(\d+)\/([^?#/]+)/);
              if (fromMatch && toMatch) {
                let fromId = parseInt(fromMatch[1]);
                let toId = parseInt(toMatch[1]);
                user.recs[fromId] = toId;
              }
            }
          });
        } else {
          getRecsPromise = Promise.resolve();
        }
      } else getRecsPromise = Promise.resolve();


      return Promise.all([
        getFriendsPromise, 
        getClubsPromise, 
        getReviewsPromise,
        getRecsPromise
      ]).then(() => {
        return user;
      });
    });
  }


  getUserSocialInfo({login}) {
    let user = {};

    // Load friends
    let getFriendsPromises = [];
    if (1) {
      user.friendsLogins = [];
      let _parseFriends = ($) => {
        let links = $('.majorPad .friendIcon a[href*="/profile/"]');
        links.each((ind, a) => {
          let match = $(a).attr('href').match(/\/profile\/([^?#/]+)/);
          if (match)
            user.friendsLogins.push(match[1]);
        });
      };
      let offset = 0;
      let limit = 100;
      let url = "https://myanimelist.net/profile/" + login + "/friends" + "?offset=" + offset;
      getFriendsPromises.push(this.loadHtml(url).then(($) => {
        _parseFriends($);
        let otherPagesPromises = [];
        let lastPageUrl = $('#content .pagination .link:last').attr('href');
        let match = lastPageUrl.match(/[?&]offset=(\d+)/);
        if (match) {
          let lastOffset = match[1];
          for (offset = limit ; offset <= lastOffset ; offset+=limit) {
            let url = "https://myanimelist.net/profile/" + login + "/friends" + "?offset=" + offset;
            otherPagesPromises.push(this.loadHtml(url).then(($) => {
              _parseFriends($);
            }));
          }
        }
        return Promise.all(otherPagesPromises);
      }));
    }
    let getFriendsPromise = Promise.all(getFriendsPromises);

    // Load clubs
    let getClubsPromise;
    if (1) {
      user.clubsIds = [];
      //no limit
      let url = "https://myanimelist.net/profile/" + login + "/clubs";
      getClubsPromise = this.loadHtml(url).then(($) => {
        let links = $('#content li a[href*="/clubs.php?cid="]');
        links.each((ind, a) => {
          let match = $(a).attr('href').match(/clubs.php\?cid=(\d+)/);
          if (match)
            user.clubsIds.push(parseInt(match[1]));
        });
      });
    } else getClubsPromise = Promise.resolve();

    return Promise.all([
      getFriendsPromise, 
      getClubsPromise,
    ]).then(() => {
      return user;
    });
  }


  getUserList({login, altJson = false}) {
    if (!altJson) {
      //better xml version
      //no pagination, shows private lists
      //BUT has DDoS protection - throws http 429
      let url = "https://myanimelist.net/malappinfo.php?u=" + login + "&status=all&type=anime";
      return this.loadXml(url).then((res) => {
        let animeList = res.myanimelist ? res.myanimelist.anime : null;
        let userInfo = res.myanimelist ? res.myanimelist.myinfo : null;
        if (res.myanimelist === undefined) {
          throw new MalError("Malformed XML. No tag <myanimelist>", 200, url, 3);
        } else if (!userInfo) {
          return null;
        } else {
          let ratings = {};
          let unratedAnimeIdsInList = [];
          let maxUpdated = 0;
          if (animeList)
            for (let item of animeList) {
              let titleId = parseInt(item.series_animedb_id);
              let rating = parseInt(item.my_score[0]);
              let updated = parseInt(item.my_last_updated[0]);
              if (updated > maxUpdated)
                maxUpdated = updated;
              if (rating > 0) {
                ratings[titleId] = rating;
              } else {
                unratedAnimeIdsInList.push(titleId);
              }
            }
          return {
            ratings: ratings,
            unratedAnimeIdsInList: unratedAnimeIdsInList,
            listUpdatedTs: maxUpdated ? new Date(maxUpdated * 1000) : null,
          };
        }
      });
    } else {
      //alternative json version
      //with pagination, throws error if list is private, no update dates
      let ratings = {};
      let unratedAnimeIdsInList = [];
      let offset = 0,
        limit = 300,
        loaded = 0,
        url;
      return Helpers.promiseDoWhile(() => { return loaded == limit; }, () => {
        url = "https://myanimelist.net/animelist/" + login + "/load.json" 
          + "?status=7&offset=" + offset;
        return this.loadJson(url).then((res) => {
          return new Promise((resolve, reject) => {
            if (res instanceof Array) {
              loaded = res.length;
              offset += loaded;
              for (let item of res) {
                if (item.score > 0) {
                  ratings[item.anime_id] = item.score;
                } else {
                  unratedAnimeIdsInList.push(item.anime_id);
                }
              }
              resolve();
            } else {
              let err;
              if (res.errors && res.errors instanceof Array) {
                let msg = (res.errors.length && res.errors[0].message) ? 
                  res.errors[0].message : ""; //"invalid request"
                err = new MalError(msg, 200, url, 3, res.errors);
              } else {
                err = new MalError("Malformed JSON", 200, url, 3);
              }
              reject(err);
            }
          });
        });
      }).then(() => {
        return {
          ratings: ratings,
          unratedAnimeIdsInList: unratedAnimeIdsInList,
        };
      });
    }
  }


  getApproxMaxAnimeId({seasonUrl = null}) {
    let url = seasonUrl ? seasonUrl : "https://myanimelist.net/anime/season/later";
    return this.loadHtml(url).then(($) => {
      let maxAnimeId = 0;
      let links = $('a[href*="/anime/"]');
      for (let i = 0 ; i < links.length ; i++) {
        let url = $(links[i]).attr('href');
        let match = url.match(/anime\/(\d+)\/([^?#/]+)/);
        if (match) {
          let titleId = parseInt(match[1]);
          if (titleId > maxAnimeId)
            maxAnimeId = titleId;
        }
      }
      if (!seasonUrl) {
        //also check last season
        url = $('a[href*="/anime/season/later"]').parent('li').prev('li').find('a').attr('href');
        return this.getApproxMaxAnimeId({seasonUrl: url}).then((_maxAnimeId) => {
          if (_maxAnimeId > maxAnimeId)
            maxAnimeId = _maxAnimeId;
          return maxAnimeId;
        });
      } else {
        return maxAnimeId;
      }
    });
  }


  getGenres() {
    let url = "https://myanimelist.net/anime.php";
    return this.loadHtml(url).then(($) => {
      let genres = {};
      let genreLinks = $('.normal_header:contains("Genres")').next('.genre-link')
        .find('a.genre-name-link[href*="/anime/genre/"]');
      for (let i = 0 ; i < genreLinks.length ; i++) {
        let url = $(genreLinks[i]).attr('href');
        let match = url.match(/genre\/(\d+)\/([^?#/]+)/);
        if (match) {
          let genreId = parseInt(match[1]);
          let genreName = match[2].replace(/_/g, ' ');
          genres[genreId] = genreName;
        }
      }

      return genres;
    });
  }


  getAnimeInfo({animeId}) {
    let url = "https://myanimelist.net/anime/" + animeId + "/" + "some_title";
    // or "https://myanimelist.net/anime.php?id=" + animeId

    return this.loadHtml(url).catch((err) => {
      if (err instanceof MalError && err.errCode == 2) {
        return null; // No MAL anime with such id!
      } else throw err;
    }).then(($) => {
      let anime = {};
      anime.id = animeId;

      // Parse name, type, genres
      let name = $('#contentWrapper h1 span[itemprop="name"]').text().trim();
      if (!name)
        name = $('.breadcrumb [itemprop="itemListElement"]:last()').text().trim();
      if (name)
        anime.name = name;

      let type = $('#content .dark_text:contains("Type:")').next().text();
      if (type)
        anime.type = type;

      let genreIds = [];
      let genreLinks = $('a[href*="/anime/genre/"]');
      for (let i = 0 ; i < genreLinks.length ; i++) {
        let url = $(genreLinks[i]).attr('href');
        let match = url.match(/genre\/(\d+)\/([^?#/]+)/);
        if (match) {
          let genreId = parseInt(match[1]);
          genreIds.push(genreId);
        }
      }
      if (genreIds)
        anime.genres = genreIds;

      // Parse franchise related animes
      anime.rels = {};
      let rels = $('.anime_detail_related_anime tr:has(a[href*="anime/"]) td.ar');
      if (rels.length) {
        for (let i = 0 ; i < rels.length ; i++) {
          let rel = $(rels[i]);
          let relName = rel.text().trim().replace(':', '');
          let animeLinks = rel.next('td').find('a[href*="anime/"]');
          if (animeLinks.length) {
            for (let j = 0 ; j < animeLinks.length ; j++) {
              let url = $(animeLinks[j]).attr('href');
              let match = url.match(/anime\/(\d+)\/([^?#/]+)/);
              if (match) {
                let animeId = parseInt(match[1]);
                anime.rels[animeId] = relName;
              }
            }
          }
        }
      }

      // Load recommendations
      let getRecsPromise;
      if (0) {
        getRecsPromise = this.getAnimeUserrecs(animeId).then((recs) => {
          anime.recs = recs;
        });
      } else getRecsPromise = Promise.resolve();


      return Promise.all([
        getRecsPromise
      ]).then(() => {
        return anime;
      });
    });
  }


  getAnimeUserrecs({animeId}) {
    let url = "https://myanimelist.net/anime/" + animeId + "/" + "some_title" + "/userrecs";
    //no limit
    return this.loadHtml(url).catch((err) => {
      if (err instanceof MalError && err.errCode == 2) {
        return null; // No MAL anime with such id!
      } else throw err;
    }).then(($) => {
      let recs = {};
      let links = $('#content .picSurround a.hoverinfo_trigger[href*="/anime/"]');
      if (links.length) {
        for (let i = 0 ; i < links.length ; i++) {
          let link = $(links[i]);
          let url = link.attr('href');
          let match = url.match(/anime\/(\d+)\/([^?#/]+)/);
          if (match) {
            let animeId = parseInt(match[1]);
            let weight = 1;
            let moreUsersCnt = link.closest('.borderClass')
              .find('.js-similar-recommendations-button strong').text();
            if (moreUsersCnt)
              weight += parseInt(moreUsersCnt);
            recs[animeId] = weight;
          }
        }
      }
      return recs;
    });
  }


  scanClubs() {
    let clubs = {};
    let hasMore = true,
      page = 1; //1-based
    return Helpers.promiseDoWhile(() => { return hasMore; }, () => {
      let url = "https://myanimelist.net/clubs.php?p=" + page;
      return this.loadHtml(url).then(($) => {
        let nextPageLink = $('#content .normal_header .pagination span.link-blue-box')
          .next('a.link-blue-box');
        hasMore = (nextPageLink.length > 0);
        let trs = $('#content table.club-list tr.table-data:has(a[href*="clubs.php?cid="])');
        for (let i = 0 ; i < trs.length ; i++) {
          let tr = $(trs[i]);
          let url = tr.find('a[href*="clubs.php?cid="]').attr('href');
          let match = url.match(/clubs\.php\?cid=(\d+)/);
          if (match) {
            let clubId = parseInt(match[1]);
            let tds = tr.find('td.ac');
            let membersCntStr = $(tds[0]).text().trim();
            //let lastCommentStr = $(tds[1]).find('.di-ib').text().trim();
            //let lastPostStr = $(tds[2]).text().trim();
            if (membersCntStr) {
              let membersCnt = parseInt(membersCntStr);
              let minMembersCnt = (page < 20 ? 10 : 50);
              if (membersCnt >= minMembersCnt) {
                clubs[clubId] = {membersCnt: membersCnt};
              }
            }
          }
        }
        page++;
      });
    }).then(() => {
      return clubs;
    });
  }


  getClubInfo({clubId}) {
    let url = "https://myanimelist.net/clubs.php?cid=" + clubId;

    return this.loadHtml(url).catch((err) => {
      if (err instanceof MalError && err.errCode == 2) {
        return null; // No club with such id!
      } else throw err;
    }).then(($) => {
      if ($('#content .badresult').text()) { //"Invalid club id provided.", "This club restricts public user access."
        return null;
      } else {
        let club = {};

        // Parse name, type
        club.name = $('h1').text();
        let tmp = $('.dark_text:contains("Category:")').closest('.spaceit_pad').contents();
        if (tmp.length == 2) {
          club.type = $(tmp[1]).text().trim();
        }

        // Parse anime relations
        let links = $('.normal_header:contains("Anime Relations")')
          .nextUntil('.normal_header').find('a[href*="anime.php?id="]');
        if (links.length) {
          club.animeIds = [];
          for (let i = 0 ; i < links.length ; i++) {
            let url = $(links[i]).attr('href');
            let match = url.match(/anime.php\?id=(\d+)/);
            if (match) {
              let animeId = parseInt(match[1]);
              club.animeIds.push(animeId);
            }
          }
        }

        // Load members
        let membersCnt = 0;
        tmp = $('.dark_text:contains("Members:")').closest('.spaceit_pad').contents();
        if (tmp.length == 2) {
          membersCnt = parseInt($(tmp[1]).text().trim());
        }

        if (membersCnt) {
          club.membersLogins = [];
          let getMembersPromises = [];
          let limit = 6*6;
          for (let page = 0 ; page < Math.ceil(membersCnt / limit) ; page++) {
            let offset = page * limit;
            let url = "https://myanimelist.net/clubs.php?id=" + clubId 
              + "&action=view&t=members&show=" + offset;
            getMembersPromises.push(this.loadHtml(url).then(($) => {
              let links = $('#content .picSurround a[href*="/profile/"]');
              for (let i = 0 ; i < links.length ; i++) {
                let url = $(links[i]).attr('href');
                let match = url.match(/profile\/([^?#/]+)/);
                if (match) {
                  let login = match[1];
                  club.membersLogins.push(login);
                }
              }
            }));
          }
          return Promise.all(getMembersPromises).then(() => {
            return club;
          });
        } else {
          return club;
        }
      }
    });
  }

}
var cls = MalParser; //for using "cls.A" as like "self::A" inside class

module.exports = MalParser;
