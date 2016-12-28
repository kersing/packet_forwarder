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
    Send a becon at a regular interval without server intervention
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
#include "monitor.h"
#include "semtech_proto.h"

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

#define NB_PKT_MAX      8 /* max number of packets per fetch/send cycle */

#define MIN_LORA_PREAMB 6 /* minimum Lora preamble length for this application */
#define STD_LORA_PREAMB 8
#define MIN_FSK_PREAMB  3 /* minimum FSK preamble length for this application */
#define STD_FSK_PREAMB  4

#define STATUS_SIZE             3072
#define TX_BUFF_SIZE    ((540 * NB_PKT_MAX) + 30 + STATUS_SIZE)

/* -------------------------------------------------------------------------- */
/* --- PRIVATE FUNCTIONS DECLARATION ---------------------------------------- */

static uint16_t crc_ccit(const uint8_t * data, unsigned size);
static uint16_t crc_ccit(const uint8_t * data, unsigned size);


/* -------------------------------------------------------------------------- */
/* --- PUBLIC VARIABLES (GLOBAL) -------------------------------------------- */

extern volatile bool exit_sig;
extern volatile bool quit_sig;

/* network configuration variables */
extern uint8_t serv_count;
extern uint64_t lgwm;
extern char serv_addr[][64];
extern char serv_port_up[][8];
extern char serv_port_down[][8];
extern bool serv_live[];
extern time_t serv_contact[];
extern int keepalive_time;

extern bool fwd_valid_pkt;
extern bool fwd_error_pkt;
extern bool fwd_nocrc_pkt;

/* TX capabilities */
extern struct lgw_tx_gain_lut_s txlut; /* TX gain table */
extern uint32_t tx_freq_min[]; /* lowest frequency supported by TX chain */
extern uint32_t tx_freq_max[]; /* highest frequency supported by TX chain */

/* measurements to establish statistics */
extern pthread_mutex_t mx_meas_up;
extern uint32_t meas_nb_rx_rcv;
extern uint32_t meas_nb_rx_ok;
extern uint32_t meas_nb_rx_bad;
extern uint32_t meas_nb_rx_nocrc;
extern uint32_t meas_up_pkt_fwd;
extern uint32_t meas_up_network_byte;
extern uint32_t meas_up_payload_byte;
extern uint32_t meas_up_dgram_sent[MAX_SERVERS];
extern uint32_t meas_up_ack_rcv[MAX_SERVERS];

extern pthread_mutex_t mx_meas_dw;
extern uint32_t meas_dw_pull_sent[MAX_SERVERS];
extern uint32_t meas_dw_ack_rcv[MAX_SERVERS];
extern uint32_t meas_dw_dgram_rcv[MAX_SERVERS];
extern uint32_t meas_dw_dgram_acp[MAX_SERVERS];
extern uint32_t meas_dw_network_byte;
extern uint32_t meas_dw_payload_byte;
extern uint32_t meas_nb_tx_ok;
extern uint32_t meas_nb_tx_fail;
extern uint32_t meas_nb_tx_requested;
extern uint32_t meas_nb_tx_rejected_collision_packet;
extern uint32_t meas_nb_tx_rejected_collision_beacon;
extern uint32_t meas_nb_tx_rejected_too_late;
extern uint32_t meas_nb_tx_rejected_too_early;
extern uint32_t meas_nb_beacon_queued;
extern uint32_t meas_nb_beacon_sent;
extern uint32_t meas_nb_beacon_rejected;

extern struct coord_s reference_coord;
extern struct jit_queue_s jit_queue;

extern uint32_t autoquit_threshold;
extern int8_t antenna_gain;

extern bool beacon_enabled;
extern bool logger_enabled;
extern uint32_t beacon_period;
extern uint32_t beacon_freq_hz;

extern pthread_mutex_t mx_xcorr;
extern bool gps_ref_valid;
extern bool gps_active;
extern struct tref time_reference_gps;
extern pthread_mutex_t mx_timeref;
extern bool xtal_correct_ok;
extern double xtal_correct;

extern pthread_mutex_t mx_stat_rep;
extern char status_report[];

/* -------------------------------------------------------------------------- */
/* --- PRIVATE VARIABLES (GLOBAL) ------------------------------------------- */

/* gateway <-> MAC protocol variables */
static uint32_t net_mac_h; /* Most Significant Nibble, network order */
static uint32_t net_mac_l; /* Least Significant Nibble, network order */

/* network sockets */
static int sock_up[MAX_SERVERS]; /* sockets for upstream traffic */
static int sock_down[MAX_SERVERS]; /* sockets for downstream traffic */

/* network protocol variables */
static struct timeval push_timeout_half = {0, (PUSH_TIMEOUT_MS * 500)}; /* cut in half, critical for throughput */
static struct timeval pull_timeout = {0, (PULL_TIMEOUT_MS * 1000)}; /* non critical for throughput */

static uint16_t crc_ccit(const uint8_t * data, unsigned size) {
    const uint16_t crc_poly = 0x1021; /* CCITT */
    const uint16_t init_val = 0xFFFF; /* CCITT */
    uint16_t x = init_val;
    unsigned i, j;

    if (data == NULL)  {
        return 0;
    }

    for (i=0; i<size; ++i) {
        x ^= (uint16_t)data[i] << 8;
        for (j=0; j<8; ++j) {
            x = (x & 0x8000) ? (x<<1) ^ crc_poly : (x<<1);
        }
    }

    return x;
}

void sem_init() {
    /* network socket creation */
    struct addrinfo hints;
    struct addrinfo *result; /* store result of getaddrinfo */
    struct addrinfo *q; /* pointer to move into *result data */
    char host_name[64];
    char port_name[64];
    int ic; /* Server Loop Variable */
    int i;

    /* process some of the configuration variables */
    net_mac_h = htonl((uint32_t)(0xFFFFFFFF & (lgwm>>32)));
    net_mac_l = htonl((uint32_t)(0xFFFFFFFF &  lgwm  ));

    /* prepare hints to open network sockets */
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET; /* WA: Forcing IPv4 as AF_UNSPEC makes connection on localhost to fail */
    hints.ai_socktype = SOCK_DGRAM;

    MSG("INFO: [Semtech] Initializing protocol for %d servers\n",serv_count);

    /* Loop through all possible servers */
    for (ic = 0; ic < serv_count; ic++) {
	/* Initialize server variables */
	serv_live[ic]    = false;
	serv_contact[ic] = time(NULL);

	/* look for server address w/ upstream port */
	i = getaddrinfo(serv_addr[ic], serv_port_up[ic], &hints, &result);
	if (i != 0) {
	    MSG("ERROR: [up] getaddrinfo on address %s (PORT %s) returned %s\n", serv_addr[ic], serv_port_up[ic], gai_strerror(i));
	    /* This is no longer a fatal error. */
	    //exit(EXIT_FAILURE);
	    continue;
	}

	/* try to open socket for upstream traffic */
	for (q=result; q!=NULL; q=q->ai_next) {
	    sock_up[ic] = socket(q->ai_family, q->ai_socktype,q->ai_protocol);
	    if (sock_up[ic] == -1) continue; /* try next field */
	    else break; /* success, get out of loop */
	}
	if (q == NULL) {
	    MSG("ERROR: [up] failed to open socket to any of server %s addresses (port %s)\n", serv_addr[ic], serv_port_up[ic]);
	    i = 1;
	    for (q=result; q!=NULL; q=q->ai_next) {
		getnameinfo(q->ai_addr, q->ai_addrlen, host_name, sizeof host_name, port_name, sizeof port_name, NI_NUMERICHOST);
		MSG("INFO: [up] result %i host:%s service:%s\n", i, host_name, port_name);
		++i;
	    }
	    /* This is no longer a fatal error. */
	    //exit(EXIT_FAILURE);
	    continue;
	}

	/* connect so we can send/receive packet with the server only */
	i = connect(sock_up[ic], q->ai_addr, q->ai_addrlen);
	if (i != 0) {
	    MSG("ERROR: [up] connect on address %s (port %s) returned: %s\n", serv_addr[ic], serv_port_down[ic], strerror(errno));
	    /* This is no longer a fatal error. */
	    //exit(EXIT_FAILURE);
	    continue;
	}
	freeaddrinfo(result);

	/* look for server address w/ downstream port */
	i = getaddrinfo(serv_addr[ic], serv_port_down[ic], &hints, &result);
	if (i != 0) {
	    MSG("ERROR: [down] getaddrinfo on address %s (port %s) returned: %s\n", serv_addr[ic], serv_port_down[ic], gai_strerror(i));
	    /* This is no longer a fatal error. */
	    //exit(EXIT_FAILURE);
	    continue;
	}

	/* try to open socket for downstream traffic */
	for (q=result; q!=NULL; q=q->ai_next) {
	    sock_down[ic] = socket(q->ai_family, q->ai_socktype,q->ai_protocol);
	    if (sock_down[ic] == -1) continue; /* try next field */
	    else break; /* success, get out of loop */
	}
	if (q == NULL) {
	    MSG("ERROR: [down] failed to open socket to any of server %s addresses (port %s)\n", serv_addr[ic], serv_port_down[ic]);
	    i = 1;
	    for (q=result; q!=NULL; q=q->ai_next) {
		getnameinfo(q->ai_addr, q->ai_addrlen, host_name, sizeof host_name, port_name, sizeof port_name, NI_NUMERICHOST);
		MSG("INFO: [down] result %i host:%s service:%s\n", i, host_name, port_name);
		++i;
	    }
	    /* This is no longer a fatal error. */
	    //exit(EXIT_FAILURE);
	    continue;
	}

	/* connect so we can send/receive packet with the server only */
	i = connect(sock_down[ic], q->ai_addr, q->ai_addrlen);
	if (i != 0) {
	    MSG("ERROR: [down] connect address %s (port %s) returned: %s\n", serv_addr[ic], serv_port_down[ic], strerror(errno));
	    /* This is no longer a fatal error. */
	    //exit(EXIT_FAILURE);
	    continue;
	}
	freeaddrinfo(result);

	/* If we made it through to here, this server is live */
	serv_live[ic] = true;
	MSG("INFO: Successfully contacted server %s\n", serv_addr[ic]);
    }


    /* set upstream socket RX timeout */
    for (ic = 0; ic < serv_count; ic++) if (serv_live[ic] == true) {
	i = setsockopt(sock_up[ic], SOL_SOCKET, SO_RCVTIMEO, (void *)&push_timeout_half, sizeof push_timeout_half);
	if (i != 0) {
	    MSG("ERROR: [up] setsockopt for server %s returned %s\n", serv_addr[ic], strerror(errno));
	    exit(EXIT_FAILURE);
	}
    }
}

void sem_stop() {
    int ic;

    /* shut down network sockets */
    for (ic = 0; ic < serv_count; ic++) if (serv_live[ic] == true) {
	shutdown(sock_up[ic], SHUT_RDWR);
	shutdown(sock_down[ic], SHUT_RDWR);
    }
}


//TODO: Check if this is a proper generalization of servers!
static int send_tx_ack(int ic, uint8_t token_h, uint8_t token_l, enum jit_error_e error) {
    uint8_t buff_ack[64]; /* buffer to give feedback to server */
    int buff_index;

    /* reset buffer */
    memset(&buff_ack, 0, sizeof buff_ack);

    /* Prepare downlink feedback to be sent to server */
    buff_ack[0] = PROTOCOL_VERSION;
    buff_ack[1] = token_h;
    buff_ack[2] = token_l;
    buff_ack[3] = PKT_TX_ACK;
    *(uint32_t *)(buff_ack + 4) = net_mac_h;
    *(uint32_t *)(buff_ack + 8) = net_mac_l;
    buff_index = 12; /* 12-byte header */

    /* Put no JSON string if there is nothing to report */
    if (error != JIT_ERROR_OK) {
        /* start of JSON structure */
        memcpy((void *)(buff_ack + buff_index), (void *)"{\"txpk_ack\":{", 13);
        buff_index += 13;
        /* set downlink error status in JSON structure */
        memcpy((void *)(buff_ack + buff_index), (void *)"\"error\":", 8);
        buff_index += 8;
        switch (error) {
            case JIT_ERROR_FULL:
            case JIT_ERROR_COLLISION_PACKET:
                memcpy((void *)(buff_ack + buff_index), (void *)"\"COLLISION_PACKET\"", 18);
                buff_index += 18;
                /* update stats */
                pthread_mutex_lock(&mx_meas_dw);
                meas_nb_tx_rejected_collision_packet += 1;
                pthread_mutex_unlock(&mx_meas_dw);
                break;
            case JIT_ERROR_TOO_LATE:
                memcpy((void *)(buff_ack + buff_index), (void *)"\"TOO_LATE\"", 10);
                buff_index += 10;
                /* update stats */
                pthread_mutex_lock(&mx_meas_dw);
                meas_nb_tx_rejected_too_late += 1;
                pthread_mutex_unlock(&mx_meas_dw);
                break;
            case JIT_ERROR_TOO_EARLY:
                memcpy((void *)(buff_ack + buff_index), (void *)"\"TOO_EARLY\"", 11);
                buff_index += 11;
                /* update stats */
                pthread_mutex_lock(&mx_meas_dw);
                meas_nb_tx_rejected_too_early += 1;
                pthread_mutex_unlock(&mx_meas_dw);
                break;
            case JIT_ERROR_COLLISION_BEACON:
                memcpy((void *)(buff_ack + buff_index), (void *)"\"COLLISION_BEACON\"", 18);
                buff_index += 18;
                /* update stats */
                pthread_mutex_lock(&mx_meas_dw);
                meas_nb_tx_rejected_collision_beacon += 1;
                pthread_mutex_unlock(&mx_meas_dw);
                break;
            case JIT_ERROR_TX_FREQ:
                memcpy((void *)(buff_ack + buff_index), (void *)"\"TX_FREQ\"", 9);
                buff_index += 9;
                break;
            case JIT_ERROR_TX_POWER:
                memcpy((void *)(buff_ack + buff_index), (void *)"\"TX_POWER\"", 10);
                buff_index += 10;
                break;
            case JIT_ERROR_GPS_UNLOCKED:
                memcpy((void *)(buff_ack + buff_index), (void *)"\"GPS_UNLOCKED\"", 14);
                buff_index += 14;
                break;
            default:
                memcpy((void *)(buff_ack + buff_index), (void *)"\"UNKNOWN\"", 9);
                buff_index += 9;
                break;
        }
        /* end of JSON structure */
        memcpy((void *)(buff_ack + buff_index), (void *)"}}", 2);
        buff_index += 2;
    }

    buff_ack[buff_index] = 0; /* add string terminator, for safety */

    /* send datagram to server */
    return send(sock_down[ic], (void *)buff_ack, buff_index, 0);
}

void sem_thread_down(void* pic) {
    int i; /* loop variables */
	int ic = (int) (long) pic;

    /* configuration and metadata for an outbound packet */
    struct lgw_pkt_tx_s txpkt;
    bool sent_immediate = false; /* option to sent the packet immediately */

    /* local timekeeping variables */
    struct timespec send_time; /* time of the pull request */
    struct timespec recv_time; /* time of return from recv socket call */

    /* data buffers */
    uint8_t buff_down[1000]; /* buffer to receive downstream packets */
    uint8_t buff_req[12]; /* buffer to compose pull requests */
    int msg_len;

    /* protocol variables */
    uint8_t token_h; /* random token for acknowledgement matching */
    uint8_t token_l; /* random token for acknowledgement matching */
    bool req_ack = false; /* keep track of whether PULL_DATA was acknowledged or not */

    /* JSON parsing variables */
    JSON_Value *root_val = NULL;
    JSON_Object *txpk_obj = NULL;
    JSON_Value *val = NULL; /* needed to detect the absence of some fields */
    const char *str; /* pointer to sub-strings in the JSON data */
    short x0, x1;
    short x2, x3, x4;
    double x5, x6;

    /* variables to send on UTC timestamp */
    struct tref local_ref; /* time reference used for UTC <-> timestamp conversion */
    struct tm utc_vector; /* for collecting the elements of the UTC time */
    struct timespec utc_tx; /* UTC time that needs to be converted to timestamp */

    /* beacon variables */
    struct lgw_pkt_tx_s beacon_pkt;
    uint8_t beacon_loop;
    time_t diff_beacon_time;
    struct timespec next_beacon_gps_time; /* gps time of next beacon packet */
    struct timespec last_beacon_gps_time; /* gps time of last enqueued beacon packet */
    int retry;

    /* beacon data fields, byte 0 is Least Significant Byte */
    uint8_t field_info = 0;
    int32_t field_latitude; /* 3 bytes, derived from reference latitude */
    int32_t field_longitude; /* 3 bytes, derived from reference longitude */
    uint16_t field_crc1, field_crc2;

    /* auto-quit variable */
    uint32_t autoquit_cnt = 0; /* count the number of PULL_DATA sent since the latest PULL_ACK */

    /* Just In Time downlink */
    struct timeval current_unix_time;
    struct timeval current_concentrator_time;
    enum jit_error_e jit_result = JIT_ERROR_OK;
    enum jit_pkt_type_e downlink_type;

    /* set downstream socket RX timeout */
	i = setsockopt(sock_down[ic], SOL_SOCKET, SO_RCVTIMEO, (void *)&pull_timeout, sizeof pull_timeout);
    if (i != 0) {
		//TODO Should this failure bring the application down?
		MSG("ERROR: [down] setsockopt for server %s returned %s\n", serv_addr[ic], strerror(errno));
        exit(EXIT_FAILURE);
    }

    /* pre-fill the pull request buffer with fixed fields */
    buff_req[0] = PROTOCOL_VERSION;
    buff_req[3] = PKT_PULL_DATA;
    *(uint32_t *)(buff_req + 4) = net_mac_h;
    *(uint32_t *)(buff_req + 8) = net_mac_l;

    /* beacon variables initialization */
    last_beacon_gps_time.tv_sec = 0;
    last_beacon_gps_time.tv_nsec = 0;

	//TODO: this should only be present in one thread => make special beacon thread?
    /* beacon packet parameters */
    beacon_pkt.tx_mode = ON_GPS; /* send on PPS pulse */
    beacon_pkt.rf_chain = 0; /* antenna A */
    beacon_pkt.rf_power = 14;
    beacon_pkt.modulation = MOD_LORA;
    beacon_pkt.bandwidth = BW_125KHZ;
    beacon_pkt.datarate = DR_LORA_SF9;
    beacon_pkt.coderate = CR_LORA_4_5;
    beacon_pkt.invert_pol = false;
    beacon_pkt.preamble = 10;
    beacon_pkt.no_crc = true;
    beacon_pkt.no_header = true;
    beacon_pkt.size = 17;

    /* fixed bacon fields (little endian) */
    beacon_pkt.payload[0] = 0x0; /* RFU */
    beacon_pkt.payload[1] = 0x0; /* RFU */
    /* 2-5 : time (variable) */
    /* 6-7 : crc1 (variable) */

    /* calculate the latitude and longitude that must be publicly reported */
    field_latitude = (int32_t)((reference_coord.lat / 90.0) * (double)(1<<23));
    if (field_latitude > (int32_t)0x007FFFFF) {
        field_latitude = (int32_t)0x007FFFFF; /* +90 N is represented as 89.99999 N */
    } else if (field_latitude < (int32_t)0xFF800000) {
        field_latitude = (int32_t)0xFF800000;
    }
    field_longitude = 0x00FFFFFF & (int32_t)((reference_coord.lon / 180.0) * (double)(1<<23)); /* +180 = -180 = 0x800000 */

    /* optional beacon fields */
    beacon_pkt.payload[ 8] = field_info;
    beacon_pkt.payload[ 9] = 0xFF &  field_latitude;
    beacon_pkt.payload[10] = 0xFF & (field_latitude >>  8);
    beacon_pkt.payload[11] = 0xFF & (field_latitude >> 16);
    beacon_pkt.payload[12] = 0xFF &  field_longitude;
    beacon_pkt.payload[13] = 0xFF & (field_longitude >>  8);
    beacon_pkt.payload[14] = 0xFF & (field_longitude >> 16);

    /* CRC of the optional beacon fileds */
    field_crc2 = crc_ccit((beacon_pkt.payload + 8), 7);
    beacon_pkt.payload[15] = 0xFF &  field_crc2;
    beacon_pkt.payload[16] = 0xFF & (field_crc2 >>  8);

    while (!exit_sig && !quit_sig) {

        /* auto-quit if the threshold is crossed */
        if ((autoquit_threshold > 0) && (autoquit_cnt >= autoquit_threshold)) {
            exit_sig = true;
			MSG("INFO: [down] for server %s the last %u PULL_DATA were not ACKed, exiting down thread for this server.\n", serv_addr[ic], autoquit_threshold);
            break;
        }

        /* generate random token for request */
        token_h = (uint8_t)rand(); /* random token */
        token_l = (uint8_t)rand(); /* random token */
        buff_req[1] = token_h;
        buff_req[2] = token_l;

        /* send PULL request and record time */
		send(sock_down[ic], (void *)buff_req, sizeof buff_req, 0);
        clock_gettime(CLOCK_MONOTONIC, &send_time);
        pthread_mutex_lock(&mx_meas_dw);
        meas_dw_pull_sent[ic] += 1;
        pthread_mutex_unlock(&mx_meas_dw);
        req_ack = false;
        autoquit_cnt++;

        /* listen to packets and process them until a new PULL request must be sent */
        recv_time = send_time;
        while ((int)difftimespec(recv_time, send_time) < keepalive_time) {

            /* try to receive a datagram */
			msg_len = recv(sock_down[ic], (void *)buff_down, (sizeof buff_down)-1, 0);
            clock_gettime(CLOCK_MONOTONIC, &recv_time);

            /* Pre-allocate beacon slots in JiT queue, to check downlink collisions */
            //TODO: this should only be present in one thread => make special beacon thread?
			//TODO: beacon can also work on local time base, implement.
			beacon_loop = JIT_NUM_BEACON_IN_QUEUE - jit_queue.num_beacon;
            retry = 0;
            while (beacon_loop && (beacon_period != 0) && (beacon_enabled == true) && (gps_active == true)) {
			/* if beacon must be prepared, load it and wait for it to trigger */
			//if ((beacon_next_pps == true) && (gps_active == true))
                pthread_mutex_lock(&mx_timeref);
                /* Wait for GPS to be ready before inserting beacons in JiT queue */
                if ((gps_ref_valid == true) && (xtal_correct_ok == true)) {

                    /* compute GPS time for next beacon to come    */
                    /*   LoRaWAN: T = k*beacon_period + TBeaconDelay */
                    /*            with TBeaconDelay = [0:50ms]       */
                    if (last_beacon_gps_time.tv_sec == 0) {
                        /* if no beacon has been queued, get next slot from current UTC time */
                        diff_beacon_time = time_reference_gps.utc.tv_sec % ((time_t)beacon_period);
                        next_beacon_gps_time.tv_sec = time_reference_gps.utc.tv_sec +
                                                        ((time_t)beacon_period - diff_beacon_time);
                    } else {
                        /* if there is already a beacon, take it as reference */
                        next_beacon_gps_time.tv_sec = last_beacon_gps_time.tv_sec + beacon_period;
                    }
                    /* now we can add a beacon_period to the reference to get next beacon GPS time */
                    next_beacon_gps_time.tv_sec += (retry * beacon_period);
                    next_beacon_gps_time.tv_nsec = 0;

                    MSG_DEBUG(DEBUG_BEACON, "GPS-now : %s", ctime(&time_reference_gps.utc.tv_sec));
                    MSG_DEBUG(DEBUG_BEACON, "GPS-last: %s", ctime(&last_beacon_gps_time.tv_sec));
                    MSG_DEBUG(DEBUG_BEACON, "GPS-next: %s", ctime(&next_beacon_gps_time.tv_sec));

                    /* convert UTC time to concentrator time, and set packet counter for JiT trigger */
                    lgw_utc2cnt(time_reference_gps, next_beacon_gps_time, &(beacon_pkt.count_us));
                    pthread_mutex_unlock(&mx_timeref);

                    /* apply frequency correction to beacon TX frequency */
                    pthread_mutex_lock(&mx_xcorr);
                    beacon_pkt.freq_hz = (uint32_t)(xtal_correct * (double)beacon_freq_hz);
                    pthread_mutex_unlock(&mx_xcorr);

                    /* load time in beacon payload */
                    beacon_pkt.payload[2] = 0xFF &  next_beacon_gps_time.tv_sec;
                    beacon_pkt.payload[3] = 0xFF & (next_beacon_gps_time.tv_sec >>  8);
                    beacon_pkt.payload[4] = 0xFF & (next_beacon_gps_time.tv_sec >> 16);
                    beacon_pkt.payload[5] = 0xFF & (next_beacon_gps_time.tv_sec >> 24);

                    /* calculate CRC */
                    field_crc1 = crc_ccit(beacon_pkt.payload, 6); /* CRC for the first 6 bytes */
                    beacon_pkt.payload[6] = 0xFF & field_crc1;
                    beacon_pkt.payload[7] = 0xFF & (field_crc1 >> 8);

                    /* Insert beacon packet in JiT queue */
                    gettimeofday(&current_unix_time, NULL);
                    get_concentrator_time(&current_concentrator_time, current_unix_time);
                    jit_result = jit_enqueue(&jit_queue, &current_concentrator_time, &beacon_pkt, JIT_PKT_TYPE_BEACON);
                    if (jit_result == JIT_ERROR_OK) {
                        /* update stats */
                        pthread_mutex_lock(&mx_meas_dw);
                        meas_nb_beacon_queued += 1;
                        pthread_mutex_unlock(&mx_meas_dw);

                        /* One more beacon in the queue */
                        beacon_loop--;
                        retry = 0;
                        last_beacon_gps_time.tv_sec = next_beacon_gps_time.tv_sec; /* keep this beacon time as reference for next one to be programmed */

                        /* display beacon payload */
                        LOGGER("--- Beacon queued (count_us=%u) - payload: ---\n", beacon_pkt.count_us);
                        for (i=0; i<24; ++i) {
                        	LOGGER("0x%02X", beacon_pkt.payload[i]);
                            if (i%8 == 7) {
                            	LOGGER("\n");
                            } else {
                            	LOGGER(" - ");
                            }
                        }
                        if (i%8 != 0) {
                        	LOGGER("\n");
                        }
                        LOGGER("--- end of payload ---\n");
                    } else {
                        /* update stats */
                        pthread_mutex_lock(&mx_meas_dw);
                        if (jit_result != JIT_ERROR_COLLISION_BEACON) {
                            meas_nb_beacon_rejected += 1;
                        }
                        pthread_mutex_unlock(&mx_meas_dw);
                        /* In case previous enqueue failed, we retry one period later until it succeeds */
                        /* Note: In case the GPS has been unlocked for a while, there can be lots of retries */
                        /*       to be done from last beacon time to a new valid one */
                        retry++;
                        MSG_DEBUG(DEBUG_BEACON, "--> beacon queuing retry=%d\n", retry);
                    }
                } else {
                    pthread_mutex_unlock(&mx_timeref);
                    break;
                }
            }

            /* if no network message was received, got back to listening sock_down socket */
            if (msg_len == -1) {
                //LOGGER("WARNING: [down] recv returned %s\n", strerror(errno)); /* too verbose */
                continue;
            }

            /* if the datagram does not respect protocol, just ignore it */
            if ((msg_len < 4) || (buff_down[0] != PROTOCOL_VERSION) || ((buff_down[3] != PKT_PULL_RESP) && (buff_down[3] != PKT_PULL_ACK))) {
				//TODO Investigate why this message is logged only at shutdown, i.e. all messages produced here are collected and
				//     spit out at program termination. This can lead to an unstable application.
				LOGGER("WARNING: [down] ignoring invalid packet len=%d, protocol_version=%d, id=%d\n", msg_len, buff_down[0], buff_down[3]);
                continue;
            }

            /* if the datagram is an ACK, check token */
            if (buff_down[3] == PKT_PULL_ACK) {
                if ((buff_down[1] == token_h) && (buff_down[2] == token_l)) {
                    if (req_ack) {
                    	LOGGER("INFO: [down] for server %s duplicate ACK received :)\n",serv_addr[ic]);
                    } else { /* if that packet was not already acknowledged */
                        req_ack = true;
                        autoquit_cnt = 0;
                        pthread_mutex_lock(&mx_meas_dw);
                        meas_dw_ack_rcv[ic] += 1;
                        pthread_mutex_unlock(&mx_meas_dw);
                        LOGGER("INFO: [down] for server %s PULL_ACK received in %i ms\n", serv_addr[ic], (int)(1000 * difftimespec(recv_time, send_time)));
                    }
                } else { /* out-of-sync token */
                	LOGGER("INFO: [down] for server %s, received out-of-sync ACK\n",serv_addr[ic]);
                }
                continue;
            }


			//TODO: This might generate to much logging data. The reporting should be reevaluated and an option -q should be added.
            /* the datagram is a PULL_RESP */
            buff_down[msg_len] = 0; /* add string terminator, just to be safe */
            LOGGER("INFO: [down] for server %s serv_addr[ic] PULL_RESP received  - token[%d:%d] :)\n",serv_addr[ic], buff_down[1], buff_down[2]); /* very verbose */
			MSG_DEBUG(DEBUG_LOG,"\nJSON down: %s\n", (char *)(buff_down + 4)); /* DEBUG: display JSON payload */

            meas_dw_dgram_rcv[ic] += 1; /* count all datagrams that are received */

            /* initialize TX struct and try to parse JSON */
            memset(&txpkt, 0, sizeof txpkt);
            root_val = json_parse_string_with_comments((const char *)(buff_down + 4)); /* JSON offset */
            if (root_val == NULL) {
            	LOGGER("WARNING: [down] invalid JSON, TX aborted\n");
                continue;
            }

            /* look for JSON sub-object 'txpk' */
            txpk_obj = json_object_get_object(json_value_get_object(root_val), "txpk");
            if (txpk_obj == NULL) {
            	LOGGER("WARNING: [down] no \"txpk\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }

            /* Parse "immediate" tag, or target timestamp, or UTC time to be converted by GPS (mandatory) */
            i = json_object_get_boolean(txpk_obj,"imme"); /* can be 1 if true, 0 if false, or -1 if not a JSON boolean */
            if (i == 1) {
                /* TX procedure: send immediately */
                sent_immediate = true;
                downlink_type = JIT_PKT_TYPE_DOWNLINK_CLASS_C;
                LOGGER("INFO: [down] a packet will be sent in \"immediate\" mode\n");
            } else {
                sent_immediate = false;
                val = json_object_get_value(txpk_obj,"tmst");
                if (val != NULL) {
                    /* TX procedure: send on timestamp value */
                    txpkt.count_us = (uint32_t)json_value_get_number(val);

                    /* Concentrator timestamp is given, we consider it is a Class A downlink */
                    downlink_type = JIT_PKT_TYPE_DOWNLINK_CLASS_A;
                } else {
                    /* TX procedure: send on UTC time (converted to timestamp value) */
                    str = json_object_get_string(txpk_obj, "time");
                    if (str == NULL) {
                    	LOGGER("WARNING: [down] no mandatory \"txpk.tmst\" or \"txpk.time\" objects in JSON, TX aborted\n");
                        json_value_free(root_val);
                        continue;
                    }
					if (gps_active == true) {
                        pthread_mutex_lock(&mx_timeref);
                        if (gps_ref_valid == true) {
                            local_ref = time_reference_gps;
                            pthread_mutex_unlock(&mx_timeref);
                        } else {
                            pthread_mutex_unlock(&mx_timeref);
                            LOGGER("WARNING: [down] no valid GPS time reference yet, impossible to send packet on specific UTC time, TX aborted\n");
                            json_value_free(root_val);

                            /* send acknoledge datagram to server */
                            send_tx_ack(ic, buff_down[1], buff_down[2], JIT_ERROR_GPS_UNLOCKED);
                            continue;
                        }
                    } else {
                    	LOGGER("WARNING: [down] GPS disabled, impossible to send packet on specific UTC time, TX aborted\n");
                        json_value_free(root_val);

                        /* send acknoledge datagram to server */
                        send_tx_ack(ic, buff_down[1], buff_down[2], JIT_ERROR_GPS_UNLOCKED);
                        continue;
                    }

                    i = sscanf (str, "%4hd-%2hd-%2hdT%2hd:%2hd:%9lf", &x0, &x1, &x2, &x3, &x4, &x5);
                    if (i != 6 ) {
                    	LOGGER("WARNING: [down] \"txpk.time\" must follow ISO 8601 format, TX aborted\n");
                        json_value_free(root_val);
                        continue;
                    }
                    x5 = modf(x5, &x6); /* x6 get the integer part of x5, x5 the fractional part */
                    utc_vector.tm_year = x0 - 1900; /* years since 1900 */
                    utc_vector.tm_mon = x1 - 1; /* months since January */
                    utc_vector.tm_mday = x2; /* day of the month 1-31 */
                    utc_vector.tm_hour = x3; /* hours since midnight */
                    utc_vector.tm_min = x4; /* minutes after the hour */
                    utc_vector.tm_sec = (int)x6;
                    utc_tx.tv_sec = mktime(&utc_vector) - timezone;
                    utc_tx.tv_nsec = (long)(1e9 * x5);

                    /* transform UTC time to timestamp */
                    i = lgw_utc2cnt(local_ref, utc_tx, &(txpkt.count_us));
                    if (i != LGW_GPS_SUCCESS) {
                    	LOGGER("WARNING: [down] could not convert UTC time to timestamp, TX aborted\n");
                        json_value_free(root_val);
                        continue;
                    } else {
                    	LOGGER("INFO: [down] a packet will be sent on timestamp value %u (calculated from UTC time)\n", txpkt.count_us);
                    }

                    /* GPS timestamp is given, we consider it is a Class B downlink */
                    downlink_type = JIT_PKT_TYPE_DOWNLINK_CLASS_B;
                }
            }

            /* Parse "No CRC" flag (optional field) */
            val = json_object_get_value(txpk_obj,"ncrc");
            if (val != NULL) {
                txpkt.no_crc = (bool)json_value_get_boolean(val);
            }

            /* parse target frequency (mandatory) */
            val = json_object_get_value(txpk_obj,"freq");
            if (val == NULL) {
            	LOGGER("WARNING: [down] no mandatory \"txpk.freq\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            txpkt.freq_hz = (uint32_t)((double)(1.0e6) * json_value_get_number(val));

            /* parse RF chain used for TX (mandatory) */
            val = json_object_get_value(txpk_obj,"rfch");
            if (val == NULL) {
            	LOGGER("WARNING: [down] no mandatory \"txpk.rfch\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            txpkt.rf_chain = (uint8_t)json_value_get_number(val);

            /* parse TX power (optional field) */
            val = json_object_get_value(txpk_obj,"powe");
            if (val != NULL) {
                txpkt.rf_power = (int8_t)json_value_get_number(val) - antenna_gain;
            }

            /* Parse modulation (mandatory) */
            str = json_object_get_string(txpk_obj, "modu");
            if (str == NULL) {
            	LOGGER("WARNING: [down] no mandatory \"txpk.modu\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            if (strcmp(str, "LORA") == 0) {
                /* Lora modulation */
                txpkt.modulation = MOD_LORA;

                /* Parse Lora spreading-factor and modulation bandwidth (mandatory) */
                str = json_object_get_string(txpk_obj, "datr");
                if (str == NULL) {
                	LOGGER("WARNING: [down] no mandatory \"txpk.datr\" object in JSON, TX aborted\n");
                    json_value_free(root_val);
                    continue;
                }
                i = sscanf(str, "SF%2hdBW%3hd", &x0, &x1);
                if (i != 2) {
                	LOGGER("WARNING: [down] format error in \"txpk.datr\", TX aborted\n");
                    json_value_free(root_val);
                    continue;
                }
                switch (x0) {
                    case  7: txpkt.datarate = DR_LORA_SF7;  break;
                    case  8: txpkt.datarate = DR_LORA_SF8;  break;
                    case  9: txpkt.datarate = DR_LORA_SF9;  break;
                    case 10: txpkt.datarate = DR_LORA_SF10; break;
                    case 11: txpkt.datarate = DR_LORA_SF11; break;
                    case 12: txpkt.datarate = DR_LORA_SF12; break;
                    default:
                    	LOGGER("WARNING: [down] format error in \"txpk.datr\", invalid SF, TX aborted\n");
                        json_value_free(root_val);
                        continue;
                }
                switch (x1) {
                    case 125: txpkt.bandwidth = BW_125KHZ; break;
                    case 250: txpkt.bandwidth = BW_250KHZ; break;
                    case 500: txpkt.bandwidth = BW_500KHZ; break;
                    default:
                    	LOGGER("WARNING: [down] format error in \"txpk.datr\", invalid BW, TX aborted\n");
                        json_value_free(root_val);
                        continue;
                }

                /* Parse ECC coding rate (optional field) */
                str = json_object_get_string(txpk_obj, "codr");
                if (str == NULL) {
                	LOGGER("WARNING: [down] no mandatory \"txpk.codr\" object in json, TX aborted\n");
                    json_value_free(root_val);
                    continue;
                }
                if      (strcmp(str, "4/5") == 0) txpkt.coderate = CR_LORA_4_5;
                else if (strcmp(str, "4/6") == 0) txpkt.coderate = CR_LORA_4_6;
                else if (strcmp(str, "2/3") == 0) txpkt.coderate = CR_LORA_4_6;
                else if (strcmp(str, "4/7") == 0) txpkt.coderate = CR_LORA_4_7;
                else if (strcmp(str, "4/8") == 0) txpkt.coderate = CR_LORA_4_8;
                else if (strcmp(str, "1/2") == 0) txpkt.coderate = CR_LORA_4_8;
                else {
                	LOGGER("WARNING: [down] format error in \"txpk.codr\", TX aborted\n");
                    json_value_free(root_val);
                    continue;
                }

                /* Parse signal polarity switch (optional field) */
                val = json_object_get_value(txpk_obj,"ipol");
                if (val != NULL) {
                    txpkt.invert_pol = (bool)json_value_get_boolean(val);
                }

                /* parse Lora preamble length (optional field, optimum min value enforced) */
                val = json_object_get_value(txpk_obj,"prea");
                if (val != NULL) {
                    i = (int)json_value_get_number(val);
                    if (i >= MIN_LORA_PREAMB) {
                        txpkt.preamble = (uint16_t)i;
                    } else {
                        txpkt.preamble = (uint16_t)MIN_LORA_PREAMB;
                    }
                } else {
                    txpkt.preamble = (uint16_t)STD_LORA_PREAMB;
                }

            } else if (strcmp(str, "FSK") == 0) {
                /* FSK modulation */
                txpkt.modulation = MOD_FSK;

                /* parse FSK bitrate (mandatory) */
                val = json_object_get_value(txpk_obj,"datr");
                if (val == NULL) {
                	LOGGER("WARNING: [down] no mandatory \"txpk.datr\" object in JSON, TX aborted\n");
                    json_value_free(root_val);
                    continue;
                }
                txpkt.datarate = (uint32_t)(json_value_get_number(val));

                /* parse frequency deviation (mandatory) */
                val = json_object_get_value(txpk_obj,"fdev");
                if (val == NULL) {
                	LOGGER("WARNING: [down] no mandatory \"txpk.fdev\" object in JSON, TX aborted\n");
                    json_value_free(root_val);
                    continue;
                }
                txpkt.f_dev = (uint8_t)(json_value_get_number(val) / 1000.0); /* JSON value in Hz, txpkt.f_dev in kHz */

                /* parse FSK preamble length (optional field, optimum min value enforced) */
                val = json_object_get_value(txpk_obj,"prea");
                if (val != NULL) {
                    i = (int)json_value_get_number(val);
                    if (i >= MIN_FSK_PREAMB) {
                        txpkt.preamble = (uint16_t)i;
                    } else {
                        txpkt.preamble = (uint16_t)MIN_FSK_PREAMB;
                    }
                } else {
                    txpkt.preamble = (uint16_t)STD_FSK_PREAMB;
                }

            } else {
            	LOGGER("WARNING: [down] invalid modulation in \"txpk.modu\", TX aborted\n");
                json_value_free(root_val);
                continue;
            }

            /* Parse payload length (mandatory) */
            val = json_object_get_value(txpk_obj,"size");
            if (val == NULL) {
            	LOGGER("WARNING: [down] no mandatory \"txpk.size\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            txpkt.size = (uint16_t)json_value_get_number(val);

            /* Parse payload data (mandatory) */
            str = json_object_get_string(txpk_obj, "data");
            if (str == NULL) {
            	LOGGER("WARNING: [down] no mandatory \"txpk.data\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            i = b64_to_bin(str, strlen(str), txpkt.payload, sizeof txpkt.payload);
            if (i != txpkt.size) {
            	LOGGER("WARNING: [down] mismatch between .size and .data size once converter to binary\n");
            }

            /* free the JSON parse tree from memory */
            json_value_free(root_val);

            /* select TX mode */
            if (sent_immediate) {
                txpkt.tx_mode = IMMEDIATE;
            } else {
                txpkt.tx_mode = TIMESTAMPED;
            }

            /* record measurement data */
            pthread_mutex_lock(&mx_meas_dw);
            meas_dw_dgram_acp[ic] += 1; /* count accepted datagrams with no JSON errors */
            meas_dw_network_byte += msg_len; /* meas_dw_network_byte */
            meas_dw_payload_byte += txpkt.size;
            pthread_mutex_unlock(&mx_meas_dw);

            /* check TX parameter before trying to queue packet */
            jit_result = JIT_ERROR_OK;
            if ((txpkt.freq_hz < tx_freq_min[txpkt.rf_chain]) || (txpkt.freq_hz > tx_freq_max[txpkt.rf_chain])) {
                jit_result = JIT_ERROR_TX_FREQ;
                LOGGER("ERROR: Packet REJECTED, unsupported frequency - %u (min:%u,max:%u)\n", txpkt.freq_hz, tx_freq_min[txpkt.rf_chain], tx_freq_max[txpkt.rf_chain]);
            }
            if (jit_result == JIT_ERROR_OK) {
                for (i=0; i<txlut.size; i++) {
                    if (txlut.lut[i].rf_power == txpkt.rf_power) {
                        /* this RF power is supported, we can continue */
                        break;
                    }
                }
                if (i == txlut.size) {
                    /* this RF power is not supported */
                    jit_result = JIT_ERROR_TX_POWER;
                    LOGGER("ERROR: Packet REJECTED, unsupported RF power for TX - %d\n", txpkt.rf_power);
                }
            }

            /* insert packet to be sent into JIT queue */
            if (jit_result == JIT_ERROR_OK) {
                gettimeofday(&current_unix_time, NULL);
                get_concentrator_time(&current_concentrator_time, current_unix_time);
                jit_result = jit_enqueue(&jit_queue, &current_concentrator_time, &txpkt, downlink_type);
                if (jit_result != JIT_ERROR_OK) {
                	LOGGER("ERROR: Packet REJECTED (jit error=%d)\n", jit_result);
                }
                pthread_mutex_lock(&mx_meas_dw);
                meas_nb_tx_requested += 1;
                pthread_mutex_unlock(&mx_meas_dw);
            }

            /* Send acknowledge datagram to server */
            send_tx_ack(ic, buff_down[1], buff_down[2], jit_result);
        }
    }
    MSG("\nINFO: End of downstream thread\n");
}


void sem_data_up(int nb_pkt, struct lgw_pkt_rx_s *rxpkt, bool send_report) {
    int i, j; /* loop variables */
    int ic; /* Server Loop Variable */
    unsigned pkt_in_dgram; /* nb on Lora packet in the current datagram */

    /* allocate memory for packet fetching and processing */
    struct lgw_pkt_rx_s *p; /* pointer on a RX packet */

    /* local copy of GPS time reference */
    bool ref_ok = false; /* determine if GPS time reference must be used or not */
    struct tref local_ref; /* time reference used for UTC <-> timestamp conversion */

    /* data buffers */
    uint8_t buff_up[TX_BUFF_SIZE]; /* buffer to compose the upstream packet */
    int buff_index;
    uint8_t buff_ack[32]; /* buffer to receive acknowledges */

    /* protocol variables */
    uint8_t token_h; /* random token for acknowledgement matching */
    uint8_t token_l; /* random token for acknowledgement matching */

    /* ping measurement variables */
    struct timespec send_time;
    struct timespec recv_time;

    /* GPS synchronization variables */
    struct timespec pkt_utc_time;
    struct tm * x; /* broken-up UTC time */

    /* variables for identification */
    char iso_timestamp[24];
    time_t system_time;

    /* mote info variables */
    uint32_t mote_addr = 0;
    uint16_t mote_fcnt = 0;

    /* fill the data buffer with fixed fields */
    buff_up[0] = PROTOCOL_VERSION;
    buff_up[3] = PKT_PUSH_DATA;
    *(uint32_t *)(buff_up + 4) = net_mac_h;
    *(uint32_t *)(buff_up + 8) = net_mac_l;

    //TODO: is this okay, can time be recruited from the local system if gps is not working?
    /* get a copy of GPS time reference (avoid 1 mutex per packet) */
	    if ((nb_pkt > 0) && (gps_active == true)) {
	pthread_mutex_lock(&mx_timeref);
	ref_ok = gps_ref_valid;
	local_ref = time_reference_gps;
	pthread_mutex_unlock(&mx_timeref);
    } else {
	ref_ok = false;
    }

    /* start composing datagram with the header */
    token_h = (uint8_t)rand(); /* random token */
    token_l = (uint8_t)rand(); /* random token */
    buff_up[1] = token_h;
    buff_up[2] = token_l;
    buff_index = 12; /* 12-byte header */

    /* start of JSON structure */

    /* Make when we are, define the start of the packet array. */
    system_time = time(NULL);
    strftime(iso_timestamp, sizeof iso_timestamp, "%FT%TZ", gmtime(&system_time));
      j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, "{\"time\":\"%s\",\"rxpk\":[", iso_timestamp);
    if (j > 0) {
	buff_index += j;
    } else {
	    MSG("ERROR: [up] failed to define the transmission buffer, this is fatal, sorry.\n");
	    exit(EXIT_FAILURE);
    }

    // this has been incorporated in the above (checked!) definition.
    //memcpy((void *)(buff_up + buff_index), (void *)"{\"rxpk\":[", 9);
    //buff_index += 9;

    /* serialize Lora packets metadata and payload */
    pkt_in_dgram = 0;
    for (i=0; i < nb_pkt; ++i) {
	p = &rxpkt[i];

	/* Get mote information from current packet (addr, fcnt) */
	/* FHDR - DevAddr */
	mote_addr  = p->payload[1];
	mote_addr |= p->payload[2] << 8;
	mote_addr |= p->payload[3] << 16;
	mote_addr |= p->payload[4] << 24;
	/* FHDR - FCnt */
	mote_fcnt  = p->payload[6];
	mote_fcnt |= p->payload[7] << 8;

	/* basic packet filtering */
	/* Note that in this handling some errors can occur the should not occur. We changed these
	 * from fatal errors to transient errors. In most cases the users expect that the systems keeps
	 * alive, just starts routing the next packet */
	pthread_mutex_lock(&mx_meas_up);
	meas_nb_rx_rcv += 1;
	switch(p->status) {
	    case STAT_CRC_OK:
		meas_nb_rx_ok += 1;
		LOGGER("INFO: [up] received packet with valid CRC from mote: %08X (fcnt=%u)\n", mote_addr, mote_fcnt );
		if (!fwd_valid_pkt) {
		    pthread_mutex_unlock(&mx_meas_up);
		    continue; /* skip that packet */
		}
		break;
	    case STAT_CRC_BAD:
		meas_nb_rx_bad += 1;
		LOGGER("INFO: [up] received packet with bad CRC from mote: %08X (fcnt=%u)\n", mote_addr, mote_fcnt );
		if (!fwd_error_pkt) {
		    pthread_mutex_unlock(&mx_meas_up);
		    continue; /* skip that packet */
		}
		break;
	    case STAT_NO_CRC:
		meas_nb_rx_nocrc += 1;
		LOGGER("INFO: [up] received packet without CRC from mote: %08X (fcnt=%u)\n", mote_addr, mote_fcnt );
		if (!fwd_nocrc_pkt) {
		    pthread_mutex_unlock(&mx_meas_up);
		    continue; /* skip that packet */
		}
		break;
	    default:
		    LOGGER("WARNING: [up] received packet with unknown status %u (size %u, modulation %u, BW %u, DR %u, RSSI %.1f)\n", p->status, p->size, p->modulation, p->bandwidth, p->datarate, p->rssi);
		pthread_mutex_unlock(&mx_meas_up);
		continue; /* skip that packet */
		// exit(EXIT_FAILURE);
	}
	meas_up_pkt_fwd += 1;
	meas_up_payload_byte += p->size;
	pthread_mutex_unlock(&mx_meas_up);

	/* Start of packet, add inter-packet separator if necessary */
	if (pkt_in_dgram == 0) {
	    buff_up[buff_index] = '{';
	    ++buff_index;
	} else {
	    buff_up[buff_index] = ',';
	    buff_up[buff_index+1] = '{';
	    buff_index += 2;
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
	//TODO: Ga na of dit packet zonder gps inderdaad afwezig is.
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
		MSG("ERROR: [up] received packet with unknown status\n");
		memcpy((void *)(buff_up + buff_index), (void *)",\"stat\":?", 9);
		buff_index += 9;
		continue; /* skip that packet*/
		//exit(EXIT_FAILURE);
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
		    MSG("ERROR: [up] lora packet with unknown datarate\n");
		    memcpy((void *)(buff_up + buff_index), (void *)",\"datr\":\"SF?", 12);
		    buff_index += 12;
		    continue; /* skip that packet*/
		    //exit(EXIT_FAILURE);
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
		    MSG("ERROR: [up] lora packet with unknown bandwidth\n");
		    memcpy((void *)(buff_up + buff_index), (void *)"BW?\"", 4);
		    buff_index += 4;
		    continue; /* skip that packet*/
		    //exit(EXIT_FAILURE);
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
		    MSG("ERROR: [up] lora packet with unknown coderate\n");
		    memcpy((void *)(buff_up + buff_index), (void *)",\"codr\":\"?\"", 11);
		    buff_index += 11;
		    continue; /* skip that packet*/
		    //exit(EXIT_FAILURE);
	    }

	    /* Lora SNR, 11-13 useful chars */
	    j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"lsnr\":%.1f", p->snr);
	    if (j > 0) {
		buff_index += j;
	    } else {
		MSG("ERROR: [up] failed to add field \"lsnr\" to the transmission buffer.\n");
		continue; /* skip that packet*/
		//exit(EXIT_FAILURE);
	    }
	} else if (p->modulation == MOD_FSK) {
	    memcpy((void *)(buff_up + buff_index), (void *)",\"modu\":\"FSK\"", 13);
	    buff_index += 13;

	    /* FSK datarate, 11-14 useful chars */
	    j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"datr\":%u", p->datarate);
	    if (j > 0) {
		buff_index += j;
	    } else {
		MSG("ERROR: [up] failed to add field \"datr\" to the transmission buffer.\n");
		continue; /* skip that packet*/
		//exit(EXIT_FAILURE);
	    }
	} else {
	    MSG("ERROR: [up] received packet with unknown modulation\n");
	    continue; /* skip that packet*/
	    //exit(EXIT_FAILURE);
	}

	/* Packet RSSI, payload size, 18-23 useful chars */
	j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"rssi\":%.0f,\"size\":%u", p->rssi, p->size);
	if (j > 0) {
	    buff_index += j;
	} else {
	    MSG("ERROR: [up] failed to add field \"size\" to the transmission buffer.\n");
	    continue; /* skip that packet*/
	    //exit(EXIT_FAILURE);
	}

	/* Packet base64-encoded payload, 14-350 useful chars */
	memcpy((void *)(buff_up + buff_index), (void *)",\"data\":\"", 9);
	buff_index += 9;
	j = bin_to_b64(p->payload, p->size, (char *)(buff_up + buff_index), 341); /* 255 bytes = 340 chars in b64 + null char */
	if (j>=0) {
	    buff_index += j;
	} else {
	    MSG("ERROR: [up] failed to add field \"data\" to the transmission buffer.\n");
	    continue; /* skip that packet*/
	    //exit(EXIT_FAILURE);
	}
	buff_up[buff_index] = '"';
	++buff_index;

	/* End of packet serialization */
	buff_up[buff_index] = '}';
	++buff_index;
	++pkt_in_dgram;
    }

    /* restart fetch sequence without sending empty JSON if all packets have been filtered out */
    if (pkt_in_dgram == 0) {
	if (send_report == true) {
	    /* need to clean up the beginning of the payload */
	    buff_index -= 8; /* removes "rxpk":[ */
	} else {
	    /* all packet have been filtered out and no report, restart loop */
	    return;
	}
    } else {
	/* end of packet array */
	buff_up[buff_index] = ']';
	++buff_index;
	/* add separator if needed */
	if (send_report == true) {
	    buff_up[buff_index] = ',';
	    ++buff_index;
	}
    }

    /* add status report if a new one is available */
    if (send_report == true) {
	pthread_mutex_lock(&mx_stat_rep);
	    j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, "%s", &status_report[1])-1;
	pthread_mutex_unlock(&mx_stat_rep);
	if (j > 0) {
	    buff_index += j;
	} else {
	    /* If the status does not fit the buffer (this should NOT happen btw), just delay it for the next
	     * round. It is certainly not a reason to drop packets. Remove the last separator and give a warning. */
	    --buff_index;
	    MSG("WARNING: [up] failed to add field the status report to the transmission buffer.\n");
	    //continue; /* skip that packet*/
	    //exit(EXIT_FAILURE);
	}
    }

    /* end of JSON datagram payload */
    buff_up[buff_index] = '}';
    ++buff_index;
    buff_up[buff_index] = 0; /* add string terminator, for safety */

    MSG_DEBUG(DEBUG_LOG,"\nJSON up: %s\n", (char *)(buff_up + 12)); /* DEBUG: display JSON payload */

    /* send datagram to servers sequentially */
    // TODO make this parallel.
    for (ic = 0; ic < serv_count; ic++) if (serv_live[ic] == true) {
	send(sock_up[ic], (void *)buff_up, buff_index, 0);
	clock_gettime(CLOCK_MONOTONIC, &send_time);
	pthread_mutex_lock(&mx_meas_up);
	meas_up_dgram_sent[ic] += 1;
	meas_up_network_byte += buff_index;

	/* wait for acknowledge (in 2 times, to catch extra packets) */
	for (i=0; i<2; ++i) {
	    j = recv(sock_up[ic], (void *)buff_ack, sizeof buff_ack, 0);
	    clock_gettime(CLOCK_MONOTONIC, &recv_time);
	    if (j == -1) {
		if (errno == EAGAIN) { /* timeout */
		    continue;
		} else { /* server connection error */
		    break;
		}
	    } else if ((j < 4) || (buff_ack[0] != PROTOCOL_VERSION) || (buff_ack[3] != PKT_PUSH_ACK)) {
		LOGGER("WARNING: [up] ignored invalid non-ACL packet\n");
		continue;
	    } else if ((buff_ack[1] != token_h) || (buff_ack[2] != token_l)) {
		LOGGER("WARNING: [up] ignored out-of sync ACK packet\n");
		continue;
	    } else {
				LOGGER("INFO: [up] PUSH_ACK for server %s received in %i ms\n", serv_addr[ic], (int)(1000 * difftimespec(recv_time, send_time)));
				serv_contact[ic] = time(NULL);
		meas_up_ack_rcv[ic] += 1;
		break;
	    }
	}
	pthread_mutex_unlock(&mx_meas_up);
    }
}

// vi: sw=4
