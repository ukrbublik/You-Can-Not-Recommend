You Can (Not) Recomend 1.11

# About
Recommender system engine on NodeJS.
Uses PostgreSQL database as storage of users, items and ratings.
Made for MyAnimeList data scheme, but can be modified to use on any database with explicit ratings (see example with MovieLens data).

Uses explicit matrix factorization algorithms: ALS (primary, multi-threaded) and SGD (single-threaded).
Matrix operations are accelerated by using C++ BLAS/LAPACK libraries binded to nodejs. (https://www.npmjs.com/package/nblas-plus https://www.npmjs.com/package/vectorious-plus)
With ALS algorithm matrix factorization task is parallelized, runs in separate NodeJS worker processes. (By default 1 worker because BLAS/LAPACK is multi-threaded and utilizes all CPU threads.) Can be split to several PCs using simple implemented clustering with IPC sockets (https://www.npmjs.com/package/quick-tcp-socket).
During training user/item factors matricies data are stored in shared memory (https://www.npmjs.com/package/shm-typed-array) to be accessible by worker processes. There is also option to store them in files (if RAM is low or data is too big to fit in RAM). Persistent storage of user/item factors - files.

# Test on MovieLens data:
...


