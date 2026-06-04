/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_CONSUMER_H__
#define __SO_CONSUMER_H__

#include "ring_buffer.h"
#include "packet.h"
#include <pthread.h>

struct linie_asteptare;

typedef struct so_consumer_ctx_t {
	struct so_ring_buffer_t *producer_rb;
	//pentru scriere in fisier
	int fisier_iesire;
	//pentru sortare
	unsigned long ordine_scriere;
	pthread_mutex_t mutex_ordine;
	struct linie_asteptare *scrieri_in_asteptare;
} so_consumer_ctx_t;

int create_consumers(pthread_t *tids,
					int num_consumers,
					so_ring_buffer_t *rb,
					const char *out_filename);

#endif /* __SO_CONSUMER_H__ */
