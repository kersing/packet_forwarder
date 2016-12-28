/*
 * ttn_proto.c
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

#include "trace.h"
#include "jitqueue.h"
#include "timersync.h"
#include "loragw_hal.h"
#include "loragw_gps.h"
#include "mp_pkt_fwd.h"

#include "connector.h"

#define MIN_LORA_PREAMB 6 /* minimum Lora preamble length for this application */
#define STD_LORA_PREAMB 8
#define MIN_FSK_PREAMB  3 /* minimum FSK preamble length for this application */
#define STD_FSK_PREAMB  4

TTN *ttn = NULL; 			// TTN Connection

extern bool fwd_valid_pkt;
extern bool fwd_error_pkt;
extern bool fwd_nocrc_pkt;

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
extern uint32_t meas_nb_tx_requested;

extern char platform[];
extern char email[];
extern char description[];
extern char ttn_gateway_id[];
extern char ttn_gateway_key[];

static void ttn_downlink(Router__DownlinkMessage *msg, void *arg) {
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
				MSG("ERROR: [down] Packet REJECTED (jit error=%d)\n", jit_result);
			}
			pthread_mutex_lock(&mx_meas_dw);
			meas_nb_tx_requested += 1;
			pthread_mutex_unlock(&mx_meas_dw);
		    }
		}
		break;
	    default:
		MSG("ERROR: [TTN] down: invalid protocol %d\n", msg->protocol_configuration->protocol_case);
	        break;
	}
    }
}

void ttn_init(void) {
    MSG("INFO: [TTN] Initializing protocol\n");
    ttngwc_init(&ttn, ttn_gateway_id, &ttn_downlink, NULL);
    if (!ttn) {
    	MSG("ERROR: [TTN] Initialize failed");
	return;
    }
    MSG("INFO: [TTN] Connecting\n");
    int err = ttngwc_connect(ttn, "23.97.152.238", 1883, NULL);
    if (err != 0) {
    	MSG("ERROR: [TTN] Connection failed");
    	ttngwc_cleanup(ttn);
	return;
    }
    MSG("INFO: [TTN] Connected\n");
}

void ttn_stop(void) {
    MSG("INFO: [TTN] Disconnecting\n");
    ttngwc_disconnect(ttn);
    ttngwc_cleanup(ttn);
}

void ttn_thread_down(void* pic) {
}

void ttn_data_up(int nb_pkt, struct lgw_pkt_rx_s *rxpkt) {
    int i;
    struct lgw_pkt_rx_s *p;
    char *datarate;
    char *bandwidth;
    char dbbuf[10];
    int err;

    for (i=0; i < nb_pkt; ++i) {
    	p = &rxpkt[i];

	// Skip any packet received where modulation is not LoRa for now
	if (p->modulation != MOD_LORA) {
	    continue;
	}

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
		MSG("ERROR: [up] lora packet with unknown bandwidth\n");
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
		MSG("ERROR: [up] lora packet with unknown coderate\n");
		continue; /* skip that packet*/
	}
	lorawan.has_f_cnt = 1;
	lorawan.f_cnt = p->payload[6] | p->payload[7] << 8;
	protocol.lorawan = &lorawan;
	up.protocol_metadata = &protocol;

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
	up.gateway_metadata = &gateway;

	// send message uplink
	err = ttngwc_send_uplink(ttn, &up);
	if (err)
	    MSG("ERROR: [up] lora packet send failed\n");
	else
	    MSG("INFO: [up] lora packet send successful\n");
    }	
}

void ttn_status_up(uint32_t rx_in, uint32_t rx_ok, uint32_t tx_in, uint32_t tx_ok) {
    int err;

    Gateway__Status status = GATEWAY__STATUS__INIT;
    status.has_timestamp = 1;
    status.timestamp = time(NULL);
    status.platform = platform;
    status.contact_email = email;
    status.description = description;
    status.has_rx_in = 1;
    status.rx_in = rx_in;
    status.has_rx_ok = 1;
    status.rx_ok = rx_ok;
    status.has_tx_in = 1;
    status.tx_in = tx_in;
    status.has_tx_ok = 1;
    status.tx_ok = tx_ok;

    if (gps_fake_enable || (gps_enabled == true && gps_ref_valid == true)) {
	Gateway__GPSMetadata location = GATEWAY__GPSMETADATA__INIT;
	location.has_latitude = 1;
	location.latitude = gps_fake_enable ? reference_coord.lat : meas_gps_coord.lat;
	location.has_longitude = 1;
	location.longitude = gps_fake_enable ? reference_coord.lon : meas_gps_coord.lon;
	location.has_altitude = 1;
	location.altitude = gps_fake_enable ? reference_coord.alt : meas_gps_coord.alt;

	status.gps = &location;
    }

    err = ttngwc_send_status(ttn, &status);
    if (err)
	MSG("ERROR: [status] TTN send status failed\n");
    else
	MSG("INFO: [status] TTN send status success\n");
}


// vi: sw=4 ai
