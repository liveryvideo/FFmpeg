#include "dashenc_pool.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

/* clang-tidy complains about pthread types, because they're not included directly, but this is intended */
#define pthread_mutex_t /* NOLINT(misc-include-cleaner) */ pthread_mutex_t
#define pthread_t /* NOLINT(misc-include-cleaner) */ pthread_t
#define pthread_cond_t /* NOLINT(misc-include-cleaner) */ pthread_cond_t
#define pthread_attr_t /* NOLINT(misc-include-cleaner) */ pthread_attr_t

struct pool_queue {
	void *arg;
	char free;
	struct pool_queue *next;
};

struct pool {
	char cancelled;
	void *(*fn)(void *);
	unsigned int remaining;
	unsigned int nthreads;
	struct pool_queue *q;
	struct pool_queue *end;
	pthread_mutex_t q_mtx;
	pthread_cond_t q_cnd;
	pthread_t threads[1];
};

static void * thread(void *arg);

void * pool_start(void * (*thread_func)(void *), unsigned int threads) {
	struct pool *pool = (struct pool *) malloc(sizeof(struct pool) + (threads-1) * sizeof(pthread_t));

	pthread_mutex_init(&pool->q_mtx, NULL);
	pthread_cond_init(&pool->q_cnd, NULL);
	pool->nthreads = threads;
	pool->fn = thread_func;
	pool->cancelled = 0;
	pool->remaining = 0;
	pool->end = NULL;
	pool->q = NULL;

	for (int i = 0; i < threads; i++) {
		pthread_create(&pool->threads[i], NULL, &thread, pool);
	}

	return pool;
}

void pool_enqueue(void *poolp, void *arg, char free) {
	struct pool *pool = (struct pool *) poolp;
	struct pool_queue *poolq = (struct pool_queue *) malloc(sizeof(struct pool_queue));
	poolq->arg = arg;
	poolq->next = NULL;
	poolq->free = free;

	pthread_mutex_lock(&pool->q_mtx);
	if (pool->end != NULL) {
		pool->end->next = poolq;
	}
	if (pool->q == NULL) {
		pool->q = poolq;
	}
	pool->end = poolq;
	pool->remaining++;
	pthread_cond_signal(&pool->q_cnd);
	pthread_mutex_unlock(&pool->q_mtx);
}

void pool_wait(void *poolp) {
	struct pool *pool = (struct pool *) poolp;

	pthread_mutex_lock(&pool->q_mtx);
	while (!pool->cancelled && pool->remaining) {
		pthread_cond_wait(&pool->q_cnd, &pool->q_mtx);
	}
	pthread_mutex_unlock(&pool->q_mtx);
}

void pool_end(void *poolp) {
	struct pool *pool = (struct pool *) poolp;
	struct pool_queue *poolq = NULL;

	pool->cancelled = 1;

	pthread_mutex_lock(&pool->q_mtx);
	pthread_cond_broadcast(&pool->q_cnd);
	pthread_mutex_unlock(&pool->q_mtx);

	for (int i = 0; i < pool->nthreads; i++) {
		pthread_join(pool->threads[i], NULL);
	}

	while (pool->q != NULL) {
		poolq = pool->q;
		pool->q = poolq->next;

		if (poolq->free) {
			free(poolq->arg);
		}
		free(poolq);
	}

	free(pool);
}

static void * thread(void *arg) {
	struct pool_queue *poolq = NULL;
	struct pool *pool = (struct pool *) arg;

	while (!pool->cancelled) {
		pthread_mutex_lock(&pool->q_mtx);
		while (!pool->cancelled && pool->q == NULL) {
			pthread_cond_wait(&pool->q_cnd, &pool->q_mtx);
		}
		if (pool->cancelled) {
			pthread_mutex_unlock(&pool->q_mtx);
			return NULL;
		}
		poolq = pool->q;
		pool->q = poolq->next;
		pool->end = (poolq == pool->end ? NULL : pool->end);
		pthread_mutex_unlock(&pool->q_mtx);

		pool->fn(poolq->arg);

		if (poolq->free) {
			free(poolq->arg);
		}
		free(poolq);
		poolq = NULL;

		pthread_mutex_lock(&pool->q_mtx);
		pool->remaining--;
		pthread_cond_broadcast(&pool->q_cnd);
		pthread_mutex_unlock(&pool->q_mtx);
	}

	return NULL;
}
