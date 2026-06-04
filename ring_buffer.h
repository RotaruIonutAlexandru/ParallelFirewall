/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_RINGBUFFER_H__
#define __SO_RINGBUFFER_H__

#include <sys/types.h>
#include <string.h>
#include <semaphore.h>

typedef struct so_ring_buffer_t {
	char *data;

	size_t read_pos;
	size_t write_pos;

	size_t len;
	size_t cap;

	//am ales sa folosesc 3 semafoare pentru sincronizare
	sem_t semafor_mutex;
	sem_t semafor_gol;
	sem_t semafor_plin;

	int stopped;
	//pentru sortare
	unsigned long numar_pachet;
} so_ring_buffer_t;

int     ring_buffer_init(so_ring_buffer_t *rb, size_t cap);
ssize_t ring_buffer_enqueue(so_ring_buffer_t *rb, void *data, size_t size);
ssize_t ring_buffer_dequeue(so_ring_buffer_t *rb, void *data, size_t size);
void    ring_buffer_destroy(so_ring_buffer_t *rb);
void    ring_buffer_stop(so_ring_buffer_t *rb);

#endif /* __SO_RINGBUFFER_H__ */
