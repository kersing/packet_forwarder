/*
 * ttn_transport.c
 *
 *  Created on: Dec 5, 2016
 *      Author: Jac Kersing
 */

/* fix an issue between POSIX and C99 */
//#ifdef __MACH__
//#elif __STDC_VERSION__ >= 199901L
//    #define _XOPEN_SOURCE 600
//#else
//    #define _XOPEN_SOURCE 500
//#endif

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

#include <pthread.h>
#include <semaphore.h>

#include "mp_pkt_fwd.h"
#include "trace.h"
#include "jitqueue.h"
#include "timersync.h"
#include "loragw_hal.h"
#include "loragw_gps.h"
#include "connector.h"
#include "transport.h"
#include "ttn_transport.h"
#include "stats.h"


#define MIN_LORA_PREAMB 6 /* minimum Lora preamble length for this application */
#define STD_LORA_PREAMB 8
#define MIN_FSK_PREAMB  3 /* minimum FSK preamble length for this application */
#define STD_FSK_PREAMB  4

extern bool fwd_valid_pkt;
extern bool fwd_error_pkt;
extern bool fwd_nocrc_pkt;

extern volatile bool exit_sig;
extern volatile bool quit_sig;

/* TX capabilities */
extern struct lgw_tx_gain_lut_s txlut; /* TX gain table */
extern uint32_t tx_freq_min[]; /* lowest frequency supported by TX chain */
extern uint32_t tx_freq_max[]; /* highest frequency supported by TX chain */

extern bool gps_enabled;
extern bool gps_ref_valid;
extern bool gps_fake_enable;
extern struct coord_s meas_gps_coord;
extern struct coord_s reference_coord;
extern struct jit_queue_s jit_queue;

extern int8_t antenna_gain;

extern pthread_mutex_t mx_meas_up;
extern pthread_mutex_t mx_meas_dw;
extern pthread_mutex_t mx_queues;

extern char platform[];
extern char email[];
extern char description[];

extern Server servers[];

void ttn_init(int idx) {
    // Create upstream thread and connect
    int i = pthread_create( &servers[idx].t_up, NULL, (void * (*)(void *))ttn_upstream, (void *) (long) idx);
    if (i != 0) {
	MSG("ERROR: [ttn] failed to create upstream thread for server \"%s\"\n",servers[i].addr);
	exit(EXIT_FAILURE);
    }
    ttn_connect(idx);
}

void ttn_downlink(Router__DownlinkMessage *msg, __attribute__ ((unused)) void *arg) {
    struct lgw_pkt_tx_s txpkt;
    bool sent_immediate = false; /* option to sent the packet immediately */
    enum jit_pkt_type_e downlink_type;
    enum jit_error_e jit_result = JIT_ERROR_OK;
    struct timeval current_unix_time;
    struct timeval current_concentrator_time;
    
    int i;
    short x0, x1;

    MSG("INFO: [down] TTN received downlink %s\n",msg->has_payload ? "with payload" : "empty???");

    if (msg->has_payload) {
        MSG("INFO: [TTN] downlink %d bytes\n",(int) msg->payload.len);
	switch (msg->protocol_configuration->protocol_case) {
	    case PROTOCOL__TX_CONFIGURATION__PROTOCOL_LORAWAN: {
		    Lorawan__TxConfiguration *lora = msg->protocol_configuration->lorawan;
		    Gateway__TxConfiguration *gtw = msg->gateway_configuration;

		    // clear transmit packet
		    memset(&txpkt, 0, sizeof txpkt);
		    if (gtw->timestamp) {
			txpkt.count_us = gtw->timestamp;
			downlink_type = JIT_PKT_TYPE_DOWNLINK_CLASS_A;
		    }
		    txpkt.freq_hz = gtw->frequency;
		    txpkt.rf_chain = gtw->rf_chain;
		    txpkt.rf_power = gtw->power - antenna_gain;
		    switch (lora->modulation) {
			case LORAWAN__MODULATION__LORA:
			    txpkt.modulation = MOD_LORA;
			    break;
			default:
			    MSG("WARNING: [down] unsupported modulation\n");
		    	    return;
		    }
		    i = sscanf(lora->data_rate, "SF%2hdBW%3hd", &x0, &x1);
		    if (i != 2) {
			MSG("WARNING: [down] format error in \"data_rate\" (%s), TX aborted\n",lora->data_rate);
			return;
		    }
		    switch (x0) {
			case  7: txpkt.datarate = DR_LORA_SF7;  break;
			case  8: txpkt.datarate = DR_LORA_SF8;  break;
			case  9: txpkt.datarate = DR_LORA_SF9;  break;
			case 10: txpkt.datarate = DR_LORA_SF10; break;
			case 11: txpkt.datarate = DR_LORA_SF11; break;
			case 12: txpkt.datarate = DR_LORA_SF12; break;
			default:
			    MSG("WARNING: [down] format error in \"data_rate\" (%s), invalid SF, TX aborted\n",lora->data_rate);
			    return;
		    }
		    switch (x1) {
			case 125: txpkt.bandwidth = BW_125KHZ; break;
			case 250: txpkt.bandwidth = BW_250KHZ; break;
			case 500: txpkt.bandwidth = BW_500KHZ; break;
			default:
			    MSG("WARNING: [down] format error in \"data_rate\" (%s), invalid BW, TX aborted\n",lora->data_rate);
			    return;
		    }
		    if      (strcmp(lora->coding_rate, "4/5") == 0) txpkt.coderate = CR_LORA_4_5;
		    else if (strcmp(lora->coding_rate, "4/6") == 0) txpkt.coderate = CR_LORA_4_6;
		    else if (strcmp(lora->coding_rate, "2/3") == 0) txpkt.coderate = CR_LORA_4_6;
		    else if (strcmp(lora->coding_rate, "4/7") == 0) txpkt.coderate = CR_LORA_4_7;
		    else if (strcmp(lora->coding_rate, "4/8") == 0) txpkt.coderate = CR_LORA_4_8;
		    else if (strcmp(lora->coding_rate, "1/2") == 0) txpkt.coderate = CR_LORA_4_8;
		    else {
			MSG("WARNING: [down] format error in \"coding_rate\" (%s), TX aborted\n",lora->coding_rate);
			return;
		    } 
		    txpkt.invert_pol = gtw->polarization_inversion;
		    txpkt.preamble = (uint16_t)STD_LORA_PREAMB;
		    txpkt.size = msg->payload.len;
		    memcpy(txpkt.payload, msg->payload.data, txpkt.size < sizeof txpkt.payload ? txpkt.size : sizeof txpkt.payload);

		    /* select TX mode */
		    if (sent_immediate) {
			txpkt.tx_mode = IMMEDIATE;
		    } else {
			txpkt.tx_mode = TIMESTAMPED;
		    }

		    /* check TX parameter before trying to queue packet */
		    jit_result = JIT_ERROR_OK;
		    if ((txpkt.freq_hz < tx_freq_min[txpkt.rf_chain]) || (txpkt.freq_hz > tx_freq_max[txpkt.rf_chain])) {
			jit_result = JIT_ERROR_TX_FREQ;
			MSG("ERROR: [down] Packet REJECTED, unsupported frequency - %u (min:%u,max:%u)\n", txpkt.freq_hz, tx_freq_min[txpkt.rf_chain], tx_freq_max[txpkt.rf_chain]);
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
			    MSG("ERROR: [down] Packet REJECTED, unsupported RF power for TX - %d\n", txpkt.rf_power);
			}
		    }

		    /* insert packet to be sent into JIT queue */
		    if (jit_result == JIT_ERROR_OK) {
			gettimeofday(&current_unix_time, NULL);
			get_concentrator_time(&current_concentrator_time, current_unix_time);
			jit_result = jit_enqueue(&jit_queue, &current_concentrator_time, &txpkt, downlink_type);
			if (jit_result != JIT_ERROR_OK) {
			    switch (jit_result) {
				case JIT_ERROR_FULL:
				case JIT_ERROR_COLLISION_PACKET:
				    increment_down(TX_REJ_COLL_PACKET);
				    break;
				case JIT_ERROR_TOO_LATE:
				    increment_down(TX_REJ_TOO_LATE);
				    break;
				case JIT_ERROR_TOO_EARLY:
				    increment_down(TX_REJ_TOO_EARLY);
				    break;
				case JIT_ERROR_COLLISION_BEACON:
				    increment_down(TX_REJ_COLL_BEACON);
				    break;
			    	default:
				    break;
			    }
			    MSG("ERROR: [down] Packet REJECTED (jit error=%d %s)\n", jit_result,jit_error(jit_result));
			}
			increment_down(TX_REQUESTED);
		    }
		}
		break;
	    default:
		MSG("ERROR: [TTN] down: invalid protocol %d\n", msg->protocol_configuration->protocol_case);
	        break;
	}
    }
}

void ttn_dummy_downlink(__attribute__ ((unused)) Router__DownlinkMessage *msg, __attribute__ ((unused)) void *arg) {
    // Used when downlink is disabled for a server
}

void ttn_data_up(int idx, int nb_pkt, struct lgw_pkt_rx_s *rxpkt) {
    Queue *entry; 
    Queue *last;

    // Don't queue data for disconnected servers
    if (servers[idx].live == false) return;

    // queue data for transmission
    entry = (Queue *)malloc(sizeof(Queue));
    if (entry == NULL) {
	MSG("ERROR: [ttn] cannot allocate memory for upstream data\n");
	// should this be fatal?? Not for now
	return;
    }
    memcpy(entry->data, rxpkt, sizeof entry->data);
    entry->nbpkt = nb_pkt;
    entry->status = NULL;
    entry->next = NULL;
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

void ttn_connect(int idx) {
    int waittime = 0;
    while (true) {
	if (waittime == 0) {
	    // wait 30 seconds for next attempt
	    waittime = 30;
	} else {
	    sleep(waittime);
	    if (waittime < 300) waittime = 2 * waittime;
	}
	ttngwc_init(&servers[idx].ttn, servers[idx].gw_id, 
		    servers[idx].downstream == true ? &ttn_downlink : &ttn_dummy_downlink, NULL);
	if (!servers[idx].ttn) {
	    MSG("ERROR: [TTN] Initialize server \"%s\" failed, retry in %d seconds",servers[idx].addr,waittime);
	    servers[idx].live = false;
	    // Will this ever recover? Retry anyway...
	    continue;
	}
	int err = ttngwc_connect(servers[idx].ttn, servers[idx].addr, 1883, servers[idx].gw_key);
	if (err != 0) {
	    MSG("ERROR: [TTN] Connection to server \"%s\" failed, retry in %d seconds",servers[idx].addr,waittime);
	    ttngwc_cleanup(servers[idx].ttn);
	    servers[idx].live = false;
	    continue;
	}
	break; 
    }
    MSG("INFO: [TTN] server \"%s\" connected\n",servers[idx].addr);
    servers[idx].live = true;
}

void ttn_stop(int idx) {
    sem_post(&servers[idx].send_sem);
    MSG("INFO: [TTN] Disconnecting server \"%s\"\n",servers[idx].addr);
    servers[idx].live = false;
    pthread_join(servers[idx].t_up, NULL);
    ttngwc_disconnect(servers[idx].ttn);
    ttngwc_cleanup(servers[idx].ttn);
}

void ttn_reconnect(int idx) {
    servers[idx].live = false;
    MSG("INFO: [TTN] Reconnecting\n");
    ttngwc_disconnect(servers[idx].ttn);
    ttngwc_cleanup(servers[idx].ttn);

    // Wake send thread to force reconnect
    sem_post(&servers[idx].send_sem);
}

void ttn_upstream(void *pic) {
    int idx = (int) (long) pic;
    int i;
    struct lgw_pkt_rx_s *p, *rxpkt;
    char *datarate;
    char *bandwidth;
    char dbbuf[10];
    int err;
    uint32_t mote_addr = 0;
    time_t system_time;
    Queue *entry;

    while (!exit_sig && !quit_sig) {
	// wait for data to arrive
    	sem_wait(&servers[idx].send_sem);

	// check connection is up and running
	if (servers[idx].live == false) {
	    ttn_connect(idx);
	}

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
	for (i=0; i < entry->nbpkt; ++i) {
	    p = &rxpkt[i];

	    // Skip any packet received where modulation is not LoRa for now
	    if (p->modulation != MOD_LORA) {
		continue;
	    }

	    // basic sanity check required for USB interfaced modules
	    mote_addr  = p->payload[1];
	    mote_addr |= p->payload[2] << 8;
	    mote_addr |= p->payload[3] << 16;
	    mote_addr |= p->payload[4] << 24;
	    if (mote_addr == 0) continue;

	    switch (p->status) {
		case STAT_CRC_OK:
		    if (!fwd_valid_pkt) continue;
		    break;
		case STAT_CRC_BAD:
		    if (!fwd_error_pkt) continue;
		    break;
		case STAT_NO_CRC:
		    if (!fwd_nocrc_pkt) continue;
		    break;
		default:
		    continue;
	    }
	    Router__UplinkMessage up = ROUTER__UPLINK_MESSAGE__INIT;
	    up.has_payload = 1;
	    up.payload.len = p->size;
	    up.payload.data = p->payload;

	    // Set protocol metadata
	    Protocol__RxMetadata protocol = PROTOCOL__RX_METADATA__INIT;
	    protocol.protocol_case = PROTOCOL__RX_METADATA__PROTOCOL_LORAWAN;
	    Lorawan__Metadata lorawan = LORAWAN__METADATA__INIT;
	    lorawan.has_modulation = 1;
	    lorawan.modulation = LORAWAN__MODULATION__LORA;

	    switch (p->datarate) {
		case DR_LORA_SF7:
		    datarate="SF7";
		    break;
		case DR_LORA_SF8:
		    datarate="SF8";
		    break;
		case DR_LORA_SF9:
		    datarate="SF9";
		    break;
		case DR_LORA_SF10:
		    datarate="SF10";
		    break;
		case DR_LORA_SF11:
		    datarate="SF11";
		    break;
		case DR_LORA_SF12:
		    datarate="SF12";
		    break;
		default:
		    continue; /* skip that packet*/
	    }
	    switch (p->bandwidth) {
		case BW_125KHZ:
		    bandwidth="BW125";
		    break;
		case BW_250KHZ:
		    bandwidth="BW250";
		    break;
		case BW_500KHZ:
		    bandwidth="BW500";
		    break;
		default:
		    MSG("ERROR: [up] TTN lora packet with unknown bandwidth\n");
		    continue; /* skip that packet*/
	    }
	    sprintf(dbbuf,"%s%s",datarate,bandwidth);
	    lorawan.data_rate = dbbuf;

	    /* Packet ECC coding rate, 11-13 useful chars */
	    switch (p->coderate) {
		case CR_LORA_4_5:
		    lorawan.coding_rate="4/5";
		    break;
		case CR_LORA_4_6:
		    lorawan.coding_rate="4/6";
		    break;
		case CR_LORA_4_7:
		    lorawan.coding_rate="4/7";
		    break;
		case CR_LORA_4_8:
		    lorawan.coding_rate="4/8";
		    break;
		case 0: /* treat the CR0 case (mostly false sync) */
		    lorawan.coding_rate="OFF";
		    break;
		default:
		    MSG("ERROR: [up] TTN lora packet with unknown coderate\n");
		    continue; /* skip that packet*/
	    }
	    lorawan.has_f_cnt = 1;
	    lorawan.f_cnt = p->payload[6] | p->payload[7] << 8;
	    protocol.lorawan = &lorawan;
	    up.protocol_metadata = &protocol;

	    system_time = time(NULL);

	    // Set gateway metadata
	    Gateway__RxMetadata gateway = GATEWAY__RX_METADATA__INIT;
	    gateway.has_timestamp = 1;
	    gateway.timestamp = p->count_us;
	    gateway.has_rf_chain = 1;
	    gateway.rf_chain = p->rf_chain;
	    gateway.has_channel = 1;
	    gateway.channel = p->if_chain;
	    gateway.has_frequency = 1;
	    gateway.frequency = p->freq_hz;
	    gateway.has_rssi = 1;
	    gateway.rssi = p->rssi;
	    gateway.has_snr = 1;
	    gateway.snr = p->snr;
	    gateway.has_time = 1;
	    gateway.time = ((uint64_t) system_time) * 1000000000;
	    up.gateway_metadata = &gateway;


	    // send message uplink
	    err = ttngwc_send_uplink(servers[idx].ttn, &up);
	    if (err) {
		MSG("ERROR: [up] TTN lora send to server \"%s\" failed\n",servers[idx].addr);
		ttn_reconnect(idx);
		break;
	    }
	    else
		MSG("INFO: [up] TTN lora packet send to server \"%s\"\n",servers[idx].addr);
	}

	// free queue entry
	free(entry);
    }
}

void ttn_status_up(int idx, uint32_t rx_in, uint32_t rx_ok, uint32_t tx_in, uint32_t tx_ok) {
    int err;
    double load[3];
    struct timeval current_concentrator_time;
    struct timeval current_unix_time;
    static uint32_t tx_in_tot = 0;
    static uint32_t tx_ok_tot = 0;
    static uint32_t rx_in_tot = 0;
    static uint32_t rx_ok_tot = 0;

    // do not try to send if not connected
    if (servers[idx].live == false) return;

    rx_in_tot = rx_in_tot + rx_in;
    rx_ok_tot = rx_ok_tot + rx_ok;
    tx_in_tot = tx_in_tot + tx_in;
    tx_ok_tot = tx_ok_tot + tx_ok;

    Gateway__Status status = GATEWAY__STATUS__INIT;
    Gateway__Status__OSMetrics osmetrics = GATEWAY__STATUS__OSMETRICS__INIT;
    Gateway__GPSMetadata location = GATEWAY__GPSMETADATA__INIT;
    status.has_timestamp = 1;
    gettimeofday(&current_unix_time, NULL);
    get_concentrator_time(&current_concentrator_time, current_unix_time);
    status.timestamp = current_concentrator_time.tv_sec * 1000000UL + current_concentrator_time.tv_usec;
    status.has_time = 1;
    status.time = ((uint64_t)time(NULL)) * 1000000000;
    status.platform = platform;
    status.contact_email = email;
    status.description = description;
    status.has_rx_in = 1;
    status.rx_in = rx_in_tot;
    status.has_rx_ok = 1;
    status.rx_ok = rx_ok_tot;
    status.has_tx_in = 1;
    status.tx_in = tx_in_tot;
    status.has_tx_ok = 1;
    status.tx_ok = tx_ok_tot;

    // Get load average
    if (getloadavg(load, 3) == 3) {
	osmetrics.has_load_1 = 1;
	osmetrics.load_1 = load[0];
	osmetrics.has_load_5 = 1;
	osmetrics.load_5 = load[1];
	osmetrics.has_load_15 = 1;
	osmetrics.load_15 = load[2];
	status.os = &osmetrics;
    }

    if (gps_fake_enable || (gps_enabled == true && gps_ref_valid == true)) {
	location.has_latitude = 1;
	location.latitude = gps_fake_enable ? reference_coord.lat : meas_gps_coord.lat;
	location.has_longitude = 1;
	location.longitude = gps_fake_enable ? reference_coord.lon : meas_gps_coord.lon;
	location.has_altitude = 1;
	location.altitude = gps_fake_enable ? reference_coord.alt : meas_gps_coord.alt;

	status.gps = &location;
    }

    err = ttngwc_send_status(servers[idx].ttn, &status);
    if (err) {
	MSG("ERROR: [status] TTN send status failed\n");
	ttn_reconnect(idx);
    }
    else
	MSG("INFO: [status] TTN send status success\n");
}

// vi: sw=4 ai
