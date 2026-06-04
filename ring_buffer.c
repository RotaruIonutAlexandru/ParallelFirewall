// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"
#include <stdlib.h>

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	/* TODO: implement ring_buffer_init */
	size_t numar_pachete;
	char *buffer;

	buffer = malloc(cap);
	if (buffer == NULL)
		return -1;
	ring->data = buffer;

	ring->cap = cap;
	ring->len = 0;
	ring->read_pos = 0;
	ring->write_pos = 0;
	ring->stopped = 0;
	ring->numar_pachet = 0;

	numar_pachete = cap / 256;

	sem_init(&ring->semafor_mutex, 0, 1);
	sem_init(&ring->semafor_gol, 0, numar_pachete);
	sem_init(&ring->semafor_plin, 0, 0);

	return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: implement ring_buffer_enqueue */
	char *destinatie;
	char *sursa;
	size_t idx;

	sem_wait(&ring->semafor_gol);
	sem_wait(&ring->semafor_mutex);

	destinatie = ring->data + ring->write_pos;
	sursa = (char *)data;
	// imi fac memcpy manual
	idx = 0;
	while (idx < size) {
		destinatie[idx] = sursa[idx];
		idx = idx + 1;
	}

	ring->write_pos = ring->write_pos + size;
	if (ring->write_pos >= ring->cap)
		ring->write_pos = ring->write_pos - ring->cap;
	ring->len = ring->len + size;

	sem_post(&ring->semafor_mutex);
	sem_post(&ring->semafor_plin);

	return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: Implement ring_buffer_dequeue */
	char *sursa;
	char *destinatie;
	unsigned long ordine;
	size_t idx;

	sem_wait(&ring->semafor_plin);
	sem_wait(&ring->semafor_mutex);

	if (ring->len < 1 && ring->stopped > 0) {
		sem_post(&ring->semafor_mutex);
		sem_post(&ring->semafor_plin);
		return -1;
	}

	sursa = ring->data + ring->read_pos;
	destinatie = (char *)data;
	//lfl si aici
	idx = 0;
	while (idx < size) {
		destinatie[idx] = sursa[idx];
		idx = idx + 1;
	}

	ordine = ring->numar_pachet;
	ring->numar_pachet = ring->numar_pachet + 1;

	ring->read_pos = ring->read_pos + size;
	if (ring->read_pos >= ring->cap)
		ring->read_pos = ring->read_pos - ring->cap;
	ring->len = ring->len - size;

	sem_post(&ring->semafor_mutex);
	sem_post(&ring->semafor_gol);

	return (ssize_t)ordine;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_destroy */
	free(ring->data);
	sem_destroy(&ring->semafor_mutex);
	sem_destroy(&ring->semafor_gol);
	sem_destroy(&ring->semafor_plin);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_stop */
	sem_wait(&ring->semafor_mutex);
	ring->stopped = 1;
	sem_post(&ring->semafor_mutex);
	sem_post(&ring->semafor_plin);
}
