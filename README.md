# You Can (Not) Recomend 1.11 #

![logo.png](https://bitbucket.org/repo/X8Ao4b/images/2734498452-logo.png)

# About
Recommender system engine on NodeJS.

Uses PostgreSQL database as storage of users, items and ratings.

Made for MyAnimeList data scheme, but can be modified to use on any database with explicit ratings 
(see example with MovieLens data).

Uses explicit matrix factorization algorithms: ALS (primary, multi-threaded) and SGD (single-threaded).

Matrix operations are accelerated by using C++ BLAS/LAPACK libraries binded to nodejs. (https://www.npmjs.com/package/nblas-plus https://www.npmjs.com/package/vectorious-plus)

With ALS algorithm matrix factorization task is parallelized, runs in separate NodeJS worker processes. 
(By default 1 worker because BLAS/LAPACK is multi-threaded and utilizes all CPU threads.) 

Can be split to several PCs using simple implemented clustering with IPC sockets (https://www.npmjs.com/package/quick-tcp-socket).

During training user/item factors matricies data are stored in shared memory (https://www.npmjs.com/package/shm-typed-array) to be accessible by worker processes.

There is also option to store them in files (if RAM is low or data is too big to fit in RAM). 
Persistent storage of user/item factors - files.


# Usage
### Test on MovieLens data:
Create PostgreSQL db, import schema from `data/db-schema.sql`
Set options in `config/config-*.js`
```bash
npm install
node index.js import_ml 1m
node index.js start
```
Open in browser `http://localhost:8004/train`
See progress at stdout
After train complete - open in browser `http://localhost:8004/userid/<uid>/recommend`

### Test on MyAnimeList data:
Create PostgreSQL db, import schema from `data/db-schema.sql`
Set options in `config/config-*.js`
See `malscan` project to import data
```bash
npm install
node index.js start
```
Open in browser `http://localhost:8004/train`
See progress at stdout
To test recommendations open in browser `http://localhost:8004/user/<login in mal>/recommend`


# Using clustering
...


# Options
See `EmfBase.DefaultOptions`
...



# Performance
OS: Ubuntu 16.04 LTS, nodejs v6.9.2
PC#1: Core i5-6500, 16GB DRR4-2133, SSD Samsung 850
Laptop#1: Lenovo Z570 - Core i5-2450M, 8GB DDR2, HDD 5400rpm (connected as slave with Fast Ethernet)

### MovieLens 1m @ PC#1
6840 users, 3883 items, 1M ratings
100 factors, 10 iters, 85/10/5% split
Prepare: 106s
Times per iteration: 3.2s for users, 3.2s for items
RMSE: 0.84, normalized = ?

### MAL @ PC#1
1.75M users with lists (2.13M without), 12.7K items, 121M ratings
100 factors, 85/10/5% split
Times per iteration: 630s for users, 720s for items, ? for rmse validate, ? for rmse test
RMSE: ?, normalized = ?

### MAL @ PC#1+Laptop#1
...


# Thoughts
You can:
- use matrix factorization as base algo
- use item-to-item recommendations for items rated by user (todo)
- use kNN as secondary algo (use users - nearest neighbours) (todo)
- use social info (friends, clubs) (todo)
You can not:
- include in recommendations items that user already plans to watch or watched but not rated
- repeat items of same mediafranchise (todo)
