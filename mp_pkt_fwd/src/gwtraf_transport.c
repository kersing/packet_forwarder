/*
 / _____)             _              | |
( (____  _____ ____ _| |_ _____  ____| |__
 \____ \| ___ |    (_   _) ___ |/ ___)  _ \
 _____) ) ____| | | || |_| ____( (___| | | |
(______/|_____)_|_|_| \__)_____)\____)_| |_|
  (C)2013 Semtech-Cycleo

Description:
    Configure Lora concentrator and forward packets to multiple servers
    Use GPS for packet timestamping.
    Send a beacon at a regular interval without server intervention
    Processes ghost packets
    Switchable tasks.
    Suited for compilation on OSX

License: Revised BSD License, see LICENSE.TXT file include in the project
Maintainer: Michael Coracin
Maintainer for TTN: Ruud Vlaming
Modifications for multi protocol use: Jac Kersing
*/

/* fix an issue between POSIX and C99 */
#ifdef __MACH__
#elif __STDC_VERSION__ >= 199901L
    #define _XOPEN_SOURCE 600
#else
    #define _XOPEN_SOURCE 500
#endif

#include <stdint.h>         /* C99 types */
#include <stdbool.h>        /* bool type */
#include <stdio.h>          /* printf, fprintf, snprintf, fopen, fputs */

#include <string.h>         /* memset */
#include <signal.h>         /* sigaction */
#include <time.h>           /* time, clock_gettime, strftime, gmtime */
#include <sys/time.h>       /* timeval */
#include <unistd.h>         /* getopt, access */
#include <stdlib.h>         /* atoi, exit */
#include <errno.h>          /* error messages */
#include <math.h>           /* modf */
#include <assert.h>

#include <sys/socket.h>     /* socket specific definitions */
#include <netinet/in.h>     /* INET constants and stuff */
#include <arpa/inet.h>      /* IP address conversion stuff */
#include <netdb.h>          /* gai_strerror */

#include <pthread.h>
#include <semaphore.h>

#include "mp_pkt_fwd.h"
#include "trace.h"
#include "jitqueue.h"
#include "timersync.h"
#include "parson.h"
#include "base64.h"
#include "loragw_hal.h"
#include "loragw_gps.h"
#include "loragw_aux.h"
#include "loragw_reg.h"
#include "mp_pkt_fwd.h"
#include "ghost.h"
#include "connector.h"
#include "transport.h"
#include "gwtraf_transport.h"
#include "stats.h"

/* -------------------------------------------------------------------------- */
/* --- PRIVATE MACROS ------------------------------------------------------- */

#define ARRAY_SIZE(a)   (sizeof(a) / sizeof((a)[0]))
#define STRINGIFY(x)    #x
#define STR(x)          STRINGIFY(x)

/* -------------------------------------------------------------------------- */
/* --- PRIVATE CONSTANTS ---------------------------------------------------- */
#define MAX_SERVERS                 4 /* Support up to 4 servers, more does not seem realistic */
#define PROTOCOL_VERSION    2           /* v1.3 */

#define DEFAULT_KEEPALIVE   5           /* default time interval for downstream keep-alive packet */
#define PUSH_TIMEOUT_MS     100
#define PULL_TIMEOUT_MS     200

#define PKT_PUSH_DATA   0
#define PKT_PUSH_ACK    1
#define PKT_PULL_DATA   2
#define PKT_PULL_RESP   3
#define PKT_PULL_ACK    4
#define PKT_TX_ACK      5

#define STATUS_SIZE             3072
#define TX_BUFF_SIZE    ((540 * NB_PKT_MAX) + 30 + STATUS_SIZE)

/* -------------------------------------------------------------------------- */
/* --- PUBLIC VARIABLES (GLOBAL) -------------------------------------------- */

extern volatile bool exit_sig;
extern volatile bool quit_sig;

/* network configuration variables */
extern uint8_t serv_count;
extern uint64_t lgwm;
extern Server servers[];
extern int keepalive_time;

extern bool fwd_valid_pkt;
extern bool fwd_error_pkt;
extern bool fwd_nocrc_pkt;

extern struct coord_s reference_coord;
extern struct jit_queue_s jit_queue;

extern uint32_t autoquit_threshold;
extern int8_t antenna_gain;

extern pthread_mutex_t mx_queues;
extern pthread_mutex_t mx_xcorr;
extern bool gps_ref_valid;
extern bool gps_active;
extern struct tref time_reference_gps;
extern pthread_mutex_t mx_timeref;
extern bool xtal_correct_ok;
extern double xtal_correct;

/* -------------------------------------------------------------------------- */
/* --- PRIVATE VARIABLES (GLOBAL) ------------------------------------------- */

void gwtraf_init(int idx) {
    /* network socket creation */
    struct addrinfo hints;
    struct addrinfo *result; /* store result of getaddrinfo */
    struct addrinfo *q; /* pointer to move into *result data */
    char host_name[64];
    char port_name[64];
    int i;

    /* prepare hints to open network sockets */
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET; /* WA: Forcing IPv4 as AF_UNSPEC makes connection on localhost to fail */
    hints.ai_socktype = SOCK_DGRAM;

    /* Initialize server variables */
    servers[idx].live = false;
    servers[idx].contact = time(NULL);

    /* look for server address w/ upstream port */
    i = getaddrinfo(servers[idx].addr, servers[idx].port_up, &hints, &result);
    if (i != 0) {
	MSG("ERROR: [up] getaddrinfo on address %s (PORT %s) returned %s\n", servers[idx].addr, servers[idx].port_up, gai_strerror(i));
	freeaddrinfo(result);
	return;
    }

    /* try to open socket for upstream traffic */
    for (q=result; q!=NULL; q=q->ai_next) {
	servers[idx].sock_up = socket(q->ai_family, q->ai_socktype,q->ai_protocol);
	if (servers[idx].sock_up == -1) continue; /* try next field */
	else break; /* success, get out of loop */
    }
    if (q == NULL) {
	MSG("ERROR: [up] failed to open socket to any of server %s addresses (port %s)\n", servers[idx].addr, servers[idx].port_up);
	i = 1;
	for (q=result; q!=NULL; q=q->ai_next) {
	    getnameinfo(q->ai_addr, q->ai_addrlen, host_name, sizeof host_name, port_name, sizeof port_name, NI_NUMERICHOST);
	    MSG("INFO: [up] result %i host:%s service:%s\n", i, host_name, port_name);
	    ++i;
	}
	freeaddrinfo(result);
	return;
    }

    /* connect so we can send/receive packet with the server only */
    i = connect(servers[idx].sock_up, q->ai_addr, q->ai_addrlen);
    if (i != 0) {
	MSG("ERROR: [up] connect on address %s (port %s) returned: %s\n", servers[idx].addr, servers[idx].port_up, strerror(errno));
	freeaddrinfo(result);
	return;
    }

    freeaddrinfo(result);

    /* If we made it through to here, this server is live */
    servers[idx].live = true;
    MSG("INFO: Successfully contacted server %s\n", servers[idx].addr);

    i = pthread_create( &servers[idx].t_up, NULL, (void * (*)(void *))gwtraf_upstream, (void *) (long) idx);
    if (i!=0) {
    	MSG("ERROR: [semtech] failed to create upstream thread for server \"%s\"\n",servers[i].addr);
	exit(EXIT_FAILURE);
    }
}

void gwtraf_stop(int idx) {
    sem_post(&servers[idx].send_sem);
    pthread_join(servers[idx].t_up, NULL);
    if (exit_sig) {
	shutdown(servers[idx].sock_up, SHUT_RDWR);
    }
}

void gwtraf_data_up(int idx, int nb_pkt, struct lgw_pkt_rx_s *rxpkt) {
    Queue *entry;
    Queue *last;

    // queue data for transmission
    entry = (Queue *)malloc(sizeof(Queue));
    if (entry == NULL) {
        MSG("ERROR: [semtech] cannot allocate memory for upstream data\n");
        // should this be fatal?? Not for now
        return;
    }
    memcpy(entry->data, rxpkt, sizeof entry->data);
    entry->nbpkt = nb_pkt;
    entry->next = NULL;
    entry->status = NULL;
    pthread_mutex_lock(&mx_queues);
    last = servers[idx].queue;
    if (last == NULL) servers[idx].queue = entry;
    else {
        while (last->next != NULL) last = last->next;
        last->next = entry;
    }
    pthread_mutex_unlock(&mx_queues);

    // Wake send thread
    sem_post(&servers[idx].send_sem);
}

void gwtraf_upstream(void *pic) {
    Queue *entry;
    int idx = (int) (long) pic;
    int i, j; /* loop variables */
    int strt;

    /* allocate memory for packet fetching and processing */
    struct lgw_pkt_rx_s *p,*rxpkt; /* pointer on a RX packet */

    /* local copy of GPS time reference */
    bool ref_ok = false; /* determine if GPS time reference must be used or not */
    struct tref local_ref; /* time reference used for UTC <-> timestamp conversion */

    /* data buffers */
    uint8_t buff_up[TX_BUFF_SIZE]; /* buffer to compose the upstream packet */
    int buff_index;

    /* GPS synchronization variables */
    struct timespec pkt_utc_time;
    struct tm * x; /* broken-up UTC time */

    /* variables for identification */
    char iso_timestamp[24];
    time_t system_time;

    uint32_t mote_addr = 0;
    uint16_t mote_fcnt = 0;

    while (!exit_sig && !quit_sig) {
        // wait for data to arrive
        sem_wait(&servers[idx].send_sem);

        // dequeue data
        pthread_mutex_lock(&mx_queues);
        entry = servers[idx].queue;
        if (entry == NULL) {
            pthread_mutex_unlock(&mx_queues);
            continue;
        }
        servers[idx].queue = entry->next;
        pthread_mutex_unlock(&mx_queues);

        rxpkt = entry->data;

	//TODO: is this okay, can time be recruited from the local system if gps is not working?
	/* get a copy of GPS time reference (avoid 1 mutex per packet) */
		if ((entry->nbpkt > 0) && (gps_active == true)) {
	    pthread_mutex_lock(&mx_timeref);
	    ref_ok = gps_ref_valid;
	    local_ref = time_reference_gps;
	    pthread_mutex_unlock(&mx_timeref);
	} else {
	    ref_ok = false;
	}

	buff_index = 0; /* 12-byte header */

	/* start of JSON structure */

	/* Make when we are, define the start of the packet array. */
	system_time = time(NULL);
	strftime(iso_timestamp, sizeof iso_timestamp, "%FT%TZ", gmtime(&system_time));
	  j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, "{\"type\":\"uplink\",\"gw\":\"%016llX\",\"time\":\"%s\",", (long long unsigned int) lgwm, iso_timestamp);
	if (j > 0) {
	    buff_index += j;
	} else {
		MSG("ERROR: [up] failed to define the transmission buffer, this is fatal, sorry.\n");
		exit(EXIT_FAILURE);
	}

	strt = buff_index;

	/* serialize one Lora packet metadata and payload */
	for (i=0; i < entry->nbpkt; ++i) {
	    p = &rxpkt[i];
	    buff_index = strt;

	    /* basic packet filtering */
	    switch(p->status) {
		case STAT_CRC_OK:
		    if (!fwd_valid_pkt) {
			continue; /* skip that packet */
		    }
		    break;
		case STAT_CRC_BAD:
		    if (!fwd_error_pkt) {
			continue; /* skip that packet */
		    }
		    break;
		case STAT_NO_CRC:
		    if (!fwd_nocrc_pkt) {
			continue; /* skip that packet */
		    }
		    break;
		default:
		    continue; /* skip that packet */
	    }

	    /* RAW timestamp, 8-17 useful chars */
	    j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, "\"tmst\":%u", p->count_us);
	    if (j > 0) {
		buff_index += j;
	    } else {
		MSG("ERROR: [up] failed to add field \"tmst\" to the transmission buffer.\n");
		continue; /* skip that packet */
		//exit(EXIT_FAILURE);
	    }

	    /* Packet RX time (GPS based), 37 useful chars */
	    if (ref_ok == true) {
		/* convert packet timestamp to UTC absolute time */
		j = lgw_cnt2utc(local_ref, p->count_us, &pkt_utc_time);
		if (j == LGW_GPS_SUCCESS) {
		    /* split the UNIX timestamp to its calendar components */
		    x = gmtime(&(pkt_utc_time.tv_sec));
		    j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"time\":\"%04i-%02i-%02iT%02i:%02i:%02i.%06liZ\"", (x->tm_year)+1900, (x->tm_mon)+1, x->tm_mday, x->tm_hour, x->tm_min, x->tm_sec, (pkt_utc_time.tv_nsec)/1000); /* ISO 8601 format */
		    if (j > 0) {
			buff_index += j;
		    } else {
			MSG("ERROR: [up] failed to add field \"time\" to the transmission buffer.\n");
			continue; /* skip that packet*/
			//exit(EXIT_FAILURE);
		    }
		}
	    }

	    /* Packet concentrator channel, RF chain & RX frequency, 34-36 useful chars */
	    j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"chan\":%1u,\"rfch\":%1u,\"freq\":%.6lf", p->if_chain, p->rf_chain, ((double)p->freq_hz / 1e6));
	    if (j > 0) {
		buff_index += j;
	    } else {
		MSG("ERROR: [up] failed to add fields \"chan\", \"rfch\", \"freq\" to the transmission buffer.\n");
		exit(EXIT_FAILURE);
	    }

	    /* Packet status, 9-10 useful chars */
	    switch (p->status) {
		case STAT_CRC_OK:
		    memcpy((void *)(buff_up + buff_index), (void *)",\"stat\":1", 9);
		    buff_index += 9;
		    break;
		case STAT_CRC_BAD:
		    memcpy((void *)(buff_up + buff_index), (void *)",\"stat\":-1", 10);
		    buff_index += 10;
		    break;
		case STAT_NO_CRC:
		    memcpy((void *)(buff_up + buff_index), (void *)",\"stat\":0", 9);
		    buff_index += 9;
		    break;
		default:
		    continue; /* skip that packet*/
	    }

	    /* Packet modulation, 13-14 useful chars */
	    if (p->modulation == MOD_LORA) {
		memcpy((void *)(buff_up + buff_index), (void *)",\"modu\":\"LORA\"", 14);
		buff_index += 14;

		/* Lora datarate & bandwidth, 16-19 useful chars */
		switch (p->datarate) {
		    case DR_LORA_SF7:
			memcpy((void *)(buff_up + buff_index), (void *)",\"datr\":\"SF7", 12);
			buff_index += 12;
			break;
		    case DR_LORA_SF8:
			memcpy((void *)(buff_up + buff_index), (void *)",\"datr\":\"SF8", 12);
			buff_index += 12;
			break;
		    case DR_LORA_SF9:
			memcpy((void *)(buff_up + buff_index), (void *)",\"datr\":\"SF9", 12);
			buff_index += 12;
			break;
		    case DR_LORA_SF10:
			memcpy((void *)(buff_up + buff_index), (void *)",\"datr\":\"SF10", 13);
			buff_index += 13;
			break;
		    case DR_LORA_SF11:
			memcpy((void *)(buff_up + buff_index), (void *)",\"datr\":\"SF11", 13);
			buff_index += 13;
			break;
		    case DR_LORA_SF12:
			memcpy((void *)(buff_up + buff_index), (void *)",\"datr\":\"SF12", 13);
			buff_index += 13;
			break;
		    default:
			continue; /* skip that packet*/
		}
		switch (p->bandwidth) {
		    case BW_125KHZ:
			memcpy((void *)(buff_up + buff_index), (void *)"BW125\"", 6);
			buff_index += 6;
			break;
		    case BW_250KHZ:
			memcpy((void *)(buff_up + buff_index), (void *)"BW250\"", 6);
			buff_index += 6;
			break;
		    case BW_500KHZ:
			memcpy((void *)(buff_up + buff_index), (void *)"BW500\"", 6);
			buff_index += 6;
			break;
		    default:
			continue; /* skip that packet*/
		}

		/* Packet ECC coding rate, 11-13 useful chars */
		switch (p->coderate) {
		    case CR_LORA_4_5:
			memcpy((void *)(buff_up + buff_index), (void *)",\"codr\":\"4/5\"", 13);
			buff_index += 13;
			break;
		    case CR_LORA_4_6:
			memcpy((void *)(buff_up + buff_index), (void *)",\"codr\":\"4/6\"", 13);
			buff_index += 13;
			break;
		    case CR_LORA_4_7:
			memcpy((void *)(buff_up + buff_index), (void *)",\"codr\":\"4/7\"", 13);
			buff_index += 13;
			break;
		    case CR_LORA_4_8:
			memcpy((void *)(buff_up + buff_index), (void *)",\"codr\":\"4/8\"", 13);
			buff_index += 13;
			break;
		    case 0: /* treat the CR0 case (mostly false sync) */
			memcpy((void *)(buff_up + buff_index), (void *)",\"codr\":\"OFF\"", 13);
			buff_index += 13;
			break;
		    default:
			continue; /* skip that packet*/
		}

		/* Lora SNR, 11-13 useful chars */
		j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"lsnr\":%.1f", p->snr);
		if (j > 0) {
		    buff_index += j;
		} else {
		    continue; /* skip that packet*/
		}
	    } else if (p->modulation == MOD_FSK) {
		memcpy((void *)(buff_up + buff_index), (void *)",\"modu\":\"FSK\"", 13);
		buff_index += 13;

		/* FSK datarate, 11-14 useful chars */
		j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"datr\":%u", p->datarate);
		if (j > 0) {
		    buff_index += j;
		} else {
		    continue; /* skip that packet*/
		}
	    } else {
		continue; /* skip that packet*/
	    }

	    /* Packet RSSI, payload size, 18-23 useful chars */
	    j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"rssi\":%.0f,\"size\":%u", p->rssi, p->size);
	    if (j > 0) {
		buff_index += j;
	    } else {
		continue; /* skip that packet*/
	    }

	    mote_addr  = p->payload[1];
	    mote_addr |= p->payload[2] << 8;
	    mote_addr |= p->payload[3] << 16;
	    mote_addr |= p->payload[4] << 24;
	    mote_fcnt  = p->payload[6];
	    mote_fcnt |= p->payload[7] << 8;

	    j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"mote\":\"%08X\",\"fcnt\":%u",
	    	mote_addr, mote_fcnt);
	    if (j > 0) {
		buff_index += j;
	    } else {
		continue; /* skip that packet*/
	    }

#if 0
	    /* TODO: */
	    /* ADD SOME BASIC PACKET INFORMATION LIKE COUNTER AND/OR JOIN REQ INFO HERE, DO NOT SEND ALL DATA */

	    /* Packet base64-encoded payload, 14-350 useful chars */
	    memcpy((void *)(buff_up + buff_index), (void *)",\"data\":\"", 9);
	    buff_index += 9;
	    j = bin_to_b64(p->payload, p->size, (char *)(buff_up + buff_index), 341); /* 255 bytes = 340 chars in b64 + null char */
	    if (j>=0) {
		buff_index += j;
	    } else {
		continue; /* skip that packet*/
	    }
	    buff_up[buff_index] = '"';
	    ++buff_index;
#endif

	    /* End of packet serialization */
	    buff_up[buff_index] = '}';
	    ++buff_index;

	    /* end of JSON datagram payload */
	    buff_up[buff_index] = 0; /* add string terminator, for safety */

	    MSG_DEBUG(DEBUG_LOG,"\nJSON up: %s\n", (char *)(buff_up + 12)); /* DEBUG: display JSON payload */

	    /* send datagram to servers sequentially */
	    send(servers[idx].sock_up, (void *)buff_up, buff_index, 0);
	}
        // free queue entry
        free(entry);
    }
}

void gwtraf_downtraf(int idx, char *json, int len) {
    send(servers[idx].sock_up, (void *)json, len, 0);
}

// vi: sw=4
