var isTest = (process.env.isTest == '1');

//----------------------------------------

var webProxies = {
  Glype: {
    'http://www.secretproxy.org': {speed: 5, stable: 1}, 
    'http://www.spysurfing.com': {speed: 5, stable: 1}, 
    'https://proxyweb.me': {speed: 4, stable: 1}, 
    'https://web-proxy.ro': {speed: 4, stable: 1}, 
    'http://proxprox.com': {speed: 4, stable: 1},
    'https://zproxy.de': {speed: 1, stable: 1},
    'https://www.magiccloak.net': {speed: 4},
    'https://0xproxy.com': {speed: 5},
    'http://www.unblock-pakistan.com': {speed: 4}, 
    'https://www.proxy62.com': {speed: 4},
    'http://unblockwebsites.us': {speed: 5},
    'http://proxyguru.info': {speed: 4},
    'http://xtcsoul.net': {speed: 5},
    'https://www.proxytime.net': {speed: 3},
    'http://7proxysites.com': {speed: 3},
    'https://www.unblocks.net': {speed: 2},
    'https://www.youcanhide.net': {speed: 2},
    'http://proxyzan.info': {speed: 1},
    'https://www.hidemenow.net': {speed: 2},
    'https://www.kproxy.asia': {speed: 1},
    'http://www.proxygogo.info': {speed: 1},
  },
  PHProxy: {
    'http://list-proxy.com': {speed: 5, stable: 1},
    'http://www.proproxy.me': {speed: 5},
    'http://ncprox.com': {speed: 3, stable: 1},
    'http://skinftw.com': {speed: 5},
    'http://hidefromyou.com': {speed: 5},
  }
};

//----------------------------------------

var proxises = {
};

//----------------------------------------

var config = {
  isTest: isTest,
	db: {
    host: "localhost",
    port: 5432,
    database: "malrec",
    user: "root",
    password: "toor"
	},
  redis: {
  },
  provider: {
    //default options for all providers, can be overwritten
    logHttp: false,
    retryTimeout: [3000, 5000],
    maxRetries: 7,
    //for type "apiClient": queueSizeConcurrent - size of client's queue, 
    // parserQueueSizeConcurrent - size of server's parser's queue
    //for "parser" - parserQueueSizeConcurrent will be used
    //queueSizeConcurrent - also number of ids to pick per grab portion
    queueSizeConcurrent: 20,
    parserQueueSizeConcurrent: 10,
  },
  providers: {
    prs: {
      type: "parser",
      addr: null,
    },
    /*
    //examples:
    loc_api_cli: {
      type: "apiClient",
      addr: "http://localhost:8800",
    },
    some_web_proxy: {
      type: "webProxy",
      webProxyType: "Glype",
    },
    some_proxy: {
      type: "proxy",
      addr: "1.1.1.1:3128",
    },
    */
  },
  scanner: {
    approxBiggestUserId: (isTest ? 130 : 5910700), //manually biggest found user id
    maxNotFoundUserIdsToStop: (isTest ? 1 : 300),
    //maxNotFoundAnimeIdsToStop: (isTest ? 1 : 100),
    log: (isTest ? true : false),
    cntErrorsToStopGrab: (isTest ? 5 : 20),
    saveProcessingIdsAfterEvery: (isTest ? 10 : 50),
  },
  tasks: {
    AllUserLogins: {
      queueSizeConcurrent: 60,
      parserQueueSizeConcurrent: 40,
      //maxHerokuInstances: 3, // 60 (max speed for heroku) / 20 (speed per 1 inst)
    },
    NewUserLists: {
      queueSizeConcurrent: 20,
      parserQueueSizeConcurrent: 3,
      //maxHerokuInstances: 50, // 60 / 1.58
    },
  },
  taskQueue: {
    retryTaskTimeout: 1000*10, //10s
    badProviderCooldownTime: 1000*60, //60s
    badResultsToMarkAsDead: 5,
  },
};

//----------------------------------------

if (1) {
  for (let i = 101 ; i <= 120 ; i++) {
    let k = 'hk'+i;
    config.providers[k] = {
      type: "apiClient",
      serverType: "heroku",
      addr: "http://mal-api-server-" + i + ".herokuapp.com",
    };
  }
}

var queueSizesForProxySpeeds = {
  5: [2, 10],
  4: [2, 10],
  3: [2, 10],
  2: [2, 10],
  1: [2, 10],
};

if (0) {
  for (let webProxyType in webProxies) {
    for (let addr in webProxies[webProxyType]) {
      let opts = webProxies[webProxyType][addr];
      if (opts.speed == 0)
        continue;
      let k = /^(https?:\/\/)?([\d\w\.-]+)/.exec(addr)[2].replace(/[.-]/g, '_');
      let conf = {
        type: "webProxy",
        webProxyType: webProxyType,
        addr: addr.replace(/\/$/, ""),
      };
      [conf.queueSizeConcurrent, conf.parserQueueSizeConcurrent] = 
        queueSizesForProxySpeeds[opts.speed];
      config.providers[k] = conf;
    }
  }
}

if (0) {
  for (let addr in proxises) {
    let opts = proxises[addr];
    if (opts.speed == 0)
      continue;
    let k = /^(https?:\/\/)?([\d\w\.-]+)/.exec(addr)[2].replace(/[.\-\:]/g, '_');
    let conf = {
      type: "proxy",
      addr: addr,
    };
    [conf.queueSizeConcurrent, conf.parserQueueSizeConcurrent] = 
      queueSizesForProxySpeeds[opts.speed];
    config.providers[k] = conf;
  }
}

module.exports = config;

