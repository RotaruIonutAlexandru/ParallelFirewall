// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

//structura pentru linia mea de asteptare
struct linie_asteptare {
	unsigned long ordine;
	int lungime;
	char continut[256];
	struct linie_asteptare *urm;
};

//convertesc hashu in hexa
static void hash_in_hex(unsigned long hash, char *sir_hash)
{
	int poz;
	unsigned long tmp;

	tmp = hash;
	poz = 15;
	while (poz >= 0) {
		int c = tmp % 16;

		if (c < 10)
			sir_hash[poz] = '0' + c;
		else
			sir_hash[poz] = 'a' + c - 10;
		tmp = tmp / 16;
		poz = poz - 1;
	}
	sir_hash[16] = '\0';
}

//timestampu il convert in zecimal
static int timestamp_in_decimal(unsigned long timestamp, char *sir_timestamp)
{
	int k;
	int poz;
	unsigned long tmp;

	if (timestamp == 0) {
		sir_timestamp[0] = '0';
		k = 1;
	} else {
		k = 0;
		tmp = timestamp;
		while (tmp > 0) {
			k = k + 1;
			tmp = tmp / 10;
		}
		poz = k - 1;
		tmp = timestamp;
		while (tmp > 0) {
			sir_timestamp[poz] = '0' + (tmp % 10);
			tmp = tmp / 10;
			poz = poz - 1;
		}
	}
	sir_timestamp[k] = '\0';
	return k;
}

static void scrie_in_fisier(so_consumer_ctx_t *ctx, const char *linie, int lungime)
{
	int total = 0;
	ssize_t rezultat;

	while (total < lungime) {
		rezultat = write(ctx->fisier_iesire, linie + total, lungime - total);
		if (rezultat <= 0)
			return;
		total = total + rezultat;
	}
}

//adauga o linie in lista de asteptare
static void adauga_in_asteptare(so_consumer_ctx_t *ctx,
				unsigned long ordine,
				const char *linie,
				int lungime)
{
	struct linie_asteptare *nod_nou;
	struct linie_asteptare *curent;
	int i;

	nod_nou = malloc(sizeof(*nod_nou));
	if (nod_nou == NULL)
		return;

	nod_nou->ordine = ordine;
	nod_nou->lungime = lungime;
	i = 0;
	while (i < lungime) {
		nod_nou->continut[i] = linie[i];
		i = i + 1;
	}
	nod_nou->urm = NULL;

	if (ctx->scrieri_in_asteptare == NULL ||
	    ordine < ctx->scrieri_in_asteptare->ordine) {
		nod_nou->urm = ctx->scrieri_in_asteptare;
		ctx->scrieri_in_asteptare = nod_nou;
		return;
	}

	curent = ctx->scrieri_in_asteptare;
	while (curent->urm != NULL && curent->urm->ordine < ordine)
		curent = curent->urm;

	nod_nou->urm = curent->urm;
	curent->urm = nod_nou;
}

//incearca sa scrie liniile care sunt gata
static void incearca_eliberare(so_consumer_ctx_t *ctx)
{
	struct linie_asteptare *nod;

	while (ctx->scrieri_in_asteptare != NULL &&
	       ctx->scrieri_in_asteptare->ordine == ctx->ordine_scriere) {
		nod = ctx->scrieri_in_asteptare;
		scrie_in_fisier(ctx, nod->continut, nod->lungime);
		ctx->ordine_scriere = ctx->ordine_scriere + 1;
		ctx->scrieri_in_asteptare = nod->urm;
		free(nod);
	}
}
//construieste linia de output
static int construieste_linie(int decizie, char *sir_hash,
			      char *sir_timestamp, int k, char *linie_output)
{
	int lungime;
	int poz;

	lungime = 0;

	if (decizie == PASS) {
		linie_output[lungime++] = 'P';
		linie_output[lungime++] = 'A';
		linie_output[lungime++] = 'S';
		linie_output[lungime++] = 'S';
	} else {
		linie_output[lungime++] = 'D';
		linie_output[lungime++] = 'R';
		linie_output[lungime++] = 'O';
		linie_output[lungime++] = 'P';
	}

	linie_output[lungime++] = ' ';

	poz = 0;
	while (poz < 16) {
		linie_output[lungime++] = sir_hash[poz];
		poz = poz + 1;
	}

	linie_output[lungime++] = ' ';

	poz = 0;
	while (poz < k) {
		linie_output[lungime++] = sir_timestamp[poz];
		poz = poz + 1;
	}

	linie_output[lungime++] = '\n';

	return lungime;
}

void consumer_thread(so_consumer_ctx_t *ctx)
{
	/* TODO: implement consumer thread */
	so_packet_t *pachet;
	char buffer[PKT_SZ];
	char linie_output[256];
	char sir_hash[17];
	char sir_timestamp[32];
	int ordine_mea;
	int decizie;
	unsigned long hash;
	unsigned long timestamp;
	int lungime;
	int k;

	do {
		ordine_mea = ring_buffer_dequeue(ctx->producer_rb, buffer, PKT_SZ);

		if (ordine_mea >= 0) {
			pachet = (so_packet_t *)buffer;

			hash = packet_hash(pachet);
			decizie = process_packet(pachet);
			timestamp = pachet->hdr.timestamp;

			hash_in_hex(hash, sir_hash);
			k = timestamp_in_decimal(timestamp, sir_timestamp);
			lungime = construieste_linie(decizie, sir_hash,
						     sir_timestamp, k, linie_output);

			pthread_mutex_lock(&ctx->mutex_ordine);
			if (ctx->ordine_scriere == (unsigned long)ordine_mea) {
				scrie_in_fisier(ctx, linie_output, lungime);
				ctx->ordine_scriere = ctx->ordine_scriere + 1;
				incearca_eliberare(ctx);
			} else {
				adauga_in_asteptare(ctx,
					(unsigned long)ordine_mea,
					linie_output,
					lungime);
			}
			pthread_mutex_unlock(&ctx->mutex_ordine);
		}
	} while (ordine_mea >= 0);

	//dupa ce ies din bucla, mai verific daca am ramas cu ceva de scris
	pthread_mutex_lock(&ctx->mutex_ordine);
	incearca_eliberare(ctx);
	pthread_mutex_unlock(&ctx->mutex_ordine);
}

int create_consumers(pthread_t *tids,
			 int num_consumers,
			 struct so_ring_buffer_t *rb,
			 const char *out_filename)
{
	so_consumer_ctx_t *date;
	int fisier_output;
	int rezultat;
	int i;

	fisier_output = open(out_filename, O_WRONLY | O_CREAT | O_TRUNC, 0777);
	if (fisier_output < 0)
		return -1;

	date = malloc(sizeof(so_consumer_ctx_t));
	if (date == NULL)
		return -1;

	date->producer_rb = rb;
	date->fisier_iesire = fisier_output;
	date->ordine_scriere = 0;
	date->scrieri_in_asteptare = NULL;
	pthread_mutex_init(&date->mutex_ordine, NULL);

	i = 0;
	while (i < num_consumers) {
		/*
		 * TODO: Launch consumer threads
		 **/
		rezultat = pthread_create(&tids[i], NULL,
			consumer_thread, date);
		if (rezultat != 0)
			return -1;
		i = i + 1;
	}

	return num_consumers;
}
