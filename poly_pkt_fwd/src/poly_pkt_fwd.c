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
*/


/* -------------------------------------------------------------------------- */
/* --- DEPENDANCIES --------------------------------------------------------- */

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
#include "poly_pkt_fwd.h"
#include "ghost.h"
#include "monitor.h"

/* -------------------------------------------------------------------------- */
/* --- PRIVATE MACROS ------------------------------------------------------- */

#define ARRAY_SIZE(a)   (sizeof(a) / sizeof((a)[0]))
#define STRINGIFY(x)    #x
#define STR(x)          STRINGIFY(x)
//#define MSG(args...)	printf(args) /* message that is destined to the user */ => moved to trace.h
//#define TRACE() 		fprintf(stderr, "@ %s %d\n", __FUNCTION__, __LINE__);

/* -------------------------------------------------------------------------- */
/* --- PRIVATE CONSTANTS ---------------------------------------------------- */

#ifndef VERSION_STRING
  #define VERSION_STRING "undefined"
#endif

#ifndef DISPLAY_PLATFORM
  #define DISPLAY_PLATFORM "undefined"
#endif

#define MAX_SERVERS		    4 /* Support up to 4 servers, more does not seem realistic */

//TODO: This default values are a code-smell, remove.
#define DEFAULT_SERVER      127.0.0.1   /* hostname also supported */
#define DEFAULT_PORT_UP     1780
#define DEFAULT_PORT_DW     1782
#define DEFAULT_KEEPALIVE   5           /* default time interval for downstream keep-alive packet */
#define DEFAULT_STAT        30          /* default time interval for statistics */
#define PUSH_TIMEOUT_MS     100
#define PULL_TIMEOUT_MS     200
#define GPS_REF_MAX_AGE     30          /* maximum admitted delay in seconds of GPS loss before considering latest GPS sync unusable */
#define FETCH_SLEEP_MS      10          /* nb of ms waited when a fetch return no packets */
#define BEACON_POLL_MS      50          /* time in ms between polling of beacon TX status */

#define PROTOCOL_VERSION    2           /* v1.3 */

#define XERR_INIT_AVG       128         /* nb of measurements the XTAL correction is averaged on as initial value */
#define XERR_FILT_COEF      256         /* coefficient for low-pass XTAL error tracking */

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

#define STATUS_SIZE		2048 //
#define TX_BUFF_SIZE    ((540 * NB_PKT_MAX) + 30 + STATUS_SIZE)

/* -------------------------------------------------------------------------- */
/* --- PRIVATE VARIABLES (GLOBAL) ------------------------------------------- */

/* signal handling variables */
volatile bool exit_sig = false; /* 1 -> application terminates cleanly (shut down hardware, close open files, etc) */
volatile bool quit_sig = false; /* 1 -> application terminates without shutting down the hardware */

/* packets filtering configuration variables */
static bool fwd_valid_pkt = true; /* packets with PAYLOAD CRC OK are forwarded */
static bool fwd_error_pkt = false; /* packets with PAYLOAD CRC ERROR are NOT forwarded */
static bool fwd_nocrc_pkt = false; /* packets with NO PAYLOAD CRC are NOT forwarded */

/* network configuration variables */
static uint8_t serv_count = 0; /* Counter for defined servers */
static uint64_t lgwm = 0; /* Lora gateway MAC address */
static char serv_addr[MAX_SERVERS][64]; /* addresses of the server (host name or IPv4/IPv6) */
static char serv_port_up[MAX_SERVERS][8]; /* servers port for upstream traffic */
static char serv_port_down[MAX_SERVERS][8]; /* servers port for downstream traffic */
static bool serv_live[MAX_SERVERS]; /* Register if the server could be defined. */
static int keepalive_time = DEFAULT_KEEPALIVE; /* send a PULL_DATA request every X seconds, negative = disabled */

/* statistics collection configuration variables */
static unsigned stat_interval = DEFAULT_STAT; /* time interval (in sec) at which statistics are collected and displayed */

/* gateway <-> MAC protocol variables */
static uint32_t net_mac_h; /* Most Significant Nibble, network order */
static uint32_t net_mac_l; /* Least Significant Nibble, network order */

/* network sockets */
static int sock_up[MAX_SERVERS]; /* sockets for upstream traffic */
static int sock_down[MAX_SERVERS]; /* sockets for downstream traffic */

/* network protocol variables */
static struct timeval push_timeout_half = {0, (PUSH_TIMEOUT_MS * 500)}; /* cut in half, critical for throughput */
static struct timeval pull_timeout = {0, (PULL_TIMEOUT_MS * 1000)}; /* non critical for throughput */

/* hardware access control and correction */
pthread_mutex_t mx_concent = PTHREAD_MUTEX_INITIALIZER; /* control access to the concentrator */
static pthread_mutex_t mx_xcorr = PTHREAD_MUTEX_INITIALIZER; /* control access to the XTAL correction */
static bool xtal_correct_ok = false; /* set true when XTAL correction is stable enough */
static double xtal_correct = 1.0;

/* GPS configuration and synchronization */
static char gps_tty_path[64] = "\0"; /* path of the TTY port GPS is connected on */
static int gps_tty_fd = -1;          /* file descriptor of the GPS TTY port */
static bool gps_active = false;      /* is GPS present and working on the board ? */

/* GPS time reference */
static pthread_mutex_t mx_timeref = PTHREAD_MUTEX_INITIALIZER; /* control access to GPS time reference */
static bool gps_ref_valid; /* is GPS reference acceptable (ie. not too old) */
static struct tref time_reference_gps; /* time reference used for UTC <-> timestamp conversion */

/* Reference coordinates, for broadcasting (beacon) */
static struct coord_s reference_coord;

/* Enable faking the GPS coordinates of the gateway */
static bool gps_fake_enable; /* fake coordinates override real coordinates */

/* measurements to establish statistics */
static pthread_mutex_t mx_meas_up = PTHREAD_MUTEX_INITIALIZER; /* control access to the upstream measurements */
static uint32_t meas_nb_rx_rcv = 0; /* count packets received */
static uint32_t meas_nb_rx_ok = 0; /* count packets received with PAYLOAD CRC OK */
static uint32_t meas_nb_rx_bad = 0; /* count packets received with PAYLOAD CRC ERROR */
static uint32_t meas_nb_rx_nocrc = 0; /* count packets received with NO PAYLOAD CRC */
static uint32_t meas_up_pkt_fwd = 0; /* number of radio packet forwarded to the server */
static uint32_t meas_up_network_byte = 0; /* sum of UDP bytes sent for upstream traffic */
static uint32_t meas_up_payload_byte = 0; /* sum of radio payload bytes sent for upstream traffic */
static uint32_t meas_up_dgram_sent[MAX_SERVERS] = {0}; /* number of datagrams sent for upstream traffic */
static uint32_t meas_up_ack_rcv[MAX_SERVERS] = {0}; /* number of datagrams acknowledged for upstream traffic */

static pthread_mutex_t mx_meas_dw = PTHREAD_MUTEX_INITIALIZER; /* control access to the downstream measurements */
static uint32_t meas_dw_pull_sent[MAX_SERVERS] = {0}; /* number of PULL requests sent for downstream traffic */
static uint32_t meas_dw_ack_rcv[MAX_SERVERS] = {0}; /* number of PULL requests acknowledged for downstream traffic */
static uint32_t meas_dw_dgram_rcv[MAX_SERVERS] = {0}; /* count PULL response datagrams received for downstream traffic */
static uint32_t meas_dw_dgram_acp[MAX_SERVERS] = {0}; /* response datagrams that are accepted for transmission */
static uint32_t meas_dw_network_byte = 0; /* sum of UDP bytes sent for upstream traffic */
static uint32_t meas_dw_payload_byte = 0; /* sum of radio payload bytes sent for upstream traffic */
static uint32_t meas_nb_tx_ok = 0; /* count packets emitted successfully */
static uint32_t meas_nb_tx_fail = 0; /* count packets were TX failed for other reasons */
static uint32_t meas_nb_tx_requested = 0; /* count TX request from server (downlinks) */
static uint32_t meas_nb_tx_rejected_collision_packet = 0; /* count packets were TX request were rejected due to collision with another packet already programmed */
static uint32_t meas_nb_tx_rejected_collision_beacon = 0; /* count packets were TX request were rejected due to collision with a beacon already programmed */
static uint32_t meas_nb_tx_rejected_too_late = 0; /* count packets were TX request were rejected because it is too late to program it */
static uint32_t meas_nb_tx_rejected_too_early = 0; /* count packets were TX request were rejected because timestamp is too much in advance */
static uint32_t meas_nb_beacon_queued = 0; /* count beacon inserted in jit queue */
static uint32_t meas_nb_beacon_sent = 0; /* count beacon actually sent to concentrator */
static uint32_t meas_nb_beacon_rejected = 0; /* count beacon rejected for queuing */


static pthread_mutex_t mx_meas_gps = PTHREAD_MUTEX_INITIALIZER; /* control access to the GPS statistics */
static bool gps_coord_valid; /* could we get valid GPS coordinates ? */
static struct coord_s meas_gps_coord; /* GPS position of the gateway */
static struct coord_s meas_gps_err; /* GPS position of the gateway */

static pthread_mutex_t mx_stat_rep = PTHREAD_MUTEX_INITIALIZER; /* control access to the status report */
static bool report_ready = false; /* true when there is a new report to send to the server */
static char status_report[STATUS_SIZE]; /* status report as a JSON object */

/* beacon parameters */
static uint32_t beacon_period = 0; /* set beaconing period, must be a sub-multiple of 86400, the nb of sec in a day */
static uint32_t beacon_freq_hz = 0; /* TX beacon frequency, in Hz */

/* auto-quit function */
static uint32_t autoquit_threshold = 0; /* enable auto-quit after a number of non-acknowledged PULL_DATA (0 = disabled)*/

//TODO: This default values are a code-smell, remove.
static char ghost_addr[64] = "127.0.0.1"; /* address of the server (host name or IPv4/IPv6) */
static char ghost_port[8]  = "1914";      /* port to listen on */

/* Variables to make the performance of forwarder locally available. */
static char stat_format[32] = "semtech";        /* format for json statistics. */
static char stat_file[1024] = "statistics.txt"; /* name / full path of file to store results in. */
static int stat_damping = 50;        /* default damping for statistical values. */

/* Just In Time TX scheduling */
static struct jit_queue_s jit_queue;

//TODO: This default values are a code-smell, remove.
static char monitor_addr[64] = "127.0.0.1"; /* address of the server (host name or IPv4/IPv6) */
static char monitor_port[8]  = "2008";      /* port to listen on */

/* Gateway specificities */
static int8_t antenna_gain = 0;

/* Control over the separate subprocesses. Per default, the system behaves like a basic packet forwarder. */
static bool gps_enabled         = false;   /* controls the use of the GPS                      */
static bool beacon_enabled      = false;   /* controls the activation of the time beacon.      */
static bool monitor_enabled     = false;   /* controls the activation access mode.             */
bool logger_enabled      = false;   /* controls the activation of more logging          */

/* TX capabilities */
static struct lgw_tx_gain_lut_s txlut; /* TX gain table */
static uint32_t tx_freq_min[LGW_RF_CHAIN_NB]; /* lowest frequency supported by TX chain */
static uint32_t tx_freq_max[LGW_RF_CHAIN_NB]; /* highest frequency supported by TX chain */

/* Control over the separate streams. Per default, the system behaves like a basic packet forwarder. */
static bool upstream_enabled     = true;    /* controls the data flow from end-node to server         */
static bool downstream_enabled   = true;    /* controls the data flow from server to end-node         */
static bool ghoststream_enabled  = false;   /* controls the data flow from ghost-node to server       */
static bool radiostream_enabled  = true;    /* controls the data flow from radio-node to server       */
static bool statusstream_enabled = true;    /* controls the data flow of status information to server */

/* Informal status fields */
static char gateway_id[16]  = "";                /* string form of gateway mac address */
static char platform[24]    = DISPLAY_PLATFORM;  /* platform definition */
static char email[40]       = "";                /* used for contact email */
static char description[64] = "";                /* used for free form description */

/* -------------------------------------------------------------------------- */
/* --- MAC OSX Extensions  -------------------------------------------------- */

#ifdef __MACH__
int clock_gettime(int clk_id, struct timespec* t) {
	(void) clk_id;
	struct timeval now;
    int rv = gettimeofday(&now, NULL);
    if (rv) return rv;
    t->tv_sec  = now.tv_sec;
    t->tv_nsec = now.tv_usec * 1000;
    return 0;
}
#endif

/* -------------------------------------------------------------------------- */
/* --- PUBLIC FUNCTIONS DECLARATION ---------------------------------------- */

double difftimespec(struct timespec end, struct timespec beginning);

/* -------------------------------------------------------------------------- */
/* --- PRIVATE FUNCTIONS DECLARATION ---------------------------------------- */

static void sig_handler(int sigio);

static int parse_SX1301_configuration(const char * conf_file);

static int parse_gateway_configuration(const char * conf_file);

static uint16_t crc_ccit(const uint8_t * data, unsigned size);

/* threads */
void thread_up(void);
void thread_down(void* pic);
void thread_gps(void);
void thread_valid(void);
void thread_jit(void);
void thread_timersync(void);

/* -------------------------------------------------------------------------- */
/* --- PRIVATE FUNCTIONS DEFINITION ----------------------------------------- */

static void sig_handler(int sigio) {
    if (sigio == SIGQUIT) {
        quit_sig = true;
    } else if ((sigio == SIGINT) || (sigio == SIGTERM)) {
        exit_sig = true;
    }
    return;
}

static int parse_SX1301_configuration(const char * conf_file) {
    int i;
    char param_name[32]; /* used to generate variable parameter names */
    const char *str; /* used to store string value from JSON object */
    const char conf_obj_name[] = "SX1301_conf";
    JSON_Value *root_val = NULL;
    JSON_Object *conf_obj = NULL;
    JSON_Value *val = NULL;
    struct lgw_conf_board_s boardconf;
    struct lgw_conf_lbt_s lbtconf;
    struct lgw_conf_rxrf_s rfconf;
    struct lgw_conf_rxif_s ifconf;
    uint32_t sf, bw, fdev;

    /* try to parse JSON */
    root_val = json_parse_file_with_comments(conf_file);
    if (root_val == NULL) {
        MSG("ERROR: %s is not a valid JSON file\n", conf_file);
        exit(EXIT_FAILURE);
    }

    /* point to the gateway configuration object */
    conf_obj = json_object_get_object(json_value_get_object(root_val), conf_obj_name);
    if (conf_obj == NULL) {
        MSG("INFO: %s does not contain a JSON object named %s\n", conf_file, conf_obj_name);
        return -1;
    } else {
        MSG("INFO: %s does contain a JSON object named %s, parsing SX1301 parameters\n", conf_file, conf_obj_name);
    }

    /* set board configuration */
    memset(&boardconf, 0, sizeof boardconf); /* initialize configuration structure */
    val = json_object_get_value(conf_obj, "lorawan_public"); /* fetch value (if possible) */
    if (json_value_get_type(val) == JSONBoolean) {
        boardconf.lorawan_public = (bool)json_value_get_boolean(val);
    } else {
        MSG("WARNING: Data type for lorawan_public seems wrong, please check\n");
        boardconf.lorawan_public = false;
    }
    val = json_object_get_value(conf_obj, "clksrc"); /* fetch value (if possible) */
    if (json_value_get_type(val) == JSONNumber) {
        boardconf.clksrc = (uint8_t)json_value_get_number(val);
    } else {
        MSG("WARNING: Data type for clksrc seems wrong, please check\n");
        boardconf.clksrc = 0;
    }
    MSG("INFO: lorawan_public %d, clksrc %d\n", boardconf.lorawan_public, boardconf.clksrc);
    /* all parameters parsed, submitting configuration to the HAL */
    if (lgw_board_setconf(boardconf) != LGW_HAL_SUCCESS) {
        MSG("WARNING: Failed to configure board\n");
    }

    /* LBT struct*/
    memset(&lbtconf, 0, sizeof lbtconf); /* initialize configuration structure */
    val = json_object_get_value(conf_obj, "lbt_cfg"); /* fetch value (if possible) */
    if (json_value_get_type(val) != JSONObject) {
        MSG("INFO: no configuration for LBT\n");
    } else {
        val = json_object_dotget_value(conf_obj, "lbt_cfg.enable"); /* fetch value (if possible) */
        if (json_value_get_type(val) == JSONBoolean) {
            lbtconf.enable = (bool)json_value_get_boolean(val);
        } else {
            MSG("WARNING: Data type for lbt_cfg.enable seems wrong, please check\n");
            lbtconf.enable = false;
        }
        val = json_object_dotget_value(conf_obj, "lbt_cfg.rssi_target"); /* fetch value (if possible) */
        if (json_value_get_type(val) == JSONNumber) {
            lbtconf.rssi_target = (uint8_t)json_value_get_number(val);
        } else {
            MSG("WARNING: Data type for lbt_cfg.rssi_target seems wrong, please check\n");
            lbtconf.rssi_target = 0;
        }
        val = json_object_dotget_value(conf_obj, "lbt_cfg.nb_channel"); /* fetch value (if possible) */
        if (json_value_get_type(val) == JSONNumber) {
            lbtconf.nb_channel = (uint8_t)json_value_get_number(val);
        } else {
            MSG("WARNING: Data type for lbt_cfg.nb_channel seems wrong, please check\n");
            lbtconf.nb_channel = 0;
        }
        val = json_object_dotget_value(conf_obj, "lbt_cfg.start_freq"); /* fetch value (if possible) */
        if (json_value_get_type(val) == JSONNumber) {
            lbtconf.start_freq = (uint32_t)json_value_get_number(val);
        } else {
            MSG("WARNING: Data type for lbt_cfg.start_freq seems wrong, please check\n");
            lbtconf.start_freq = 0;
        }
        val = json_object_dotget_value(conf_obj, "lbt_cfg.scan_time_us"); /* fetch value (if possible) */
        if (json_value_get_type(val) == JSONNumber) {
            lbtconf.scan_time_us = (uint32_t)json_value_get_number(val);
        } else {
            MSG("WARNING: Data type for lbt_cfg.scan_time_us seems wrong, please check\n");
            lbtconf.scan_time_us = 0;
        }
        val = json_object_dotget_value(conf_obj, "lbt_cfg.tx_delay_1ch_us"); /* fetch value (if possible) */
        if (json_value_get_type(val) == JSONNumber) {
            lbtconf.tx_delay_1ch_us = (uint32_t)json_value_get_number(val);
        } else {
            MSG("WARNING: Data type for lbt_cfg.tx_delay_1ch_us seems wrong, please check\n");
            lbtconf.tx_delay_1ch_us = 0;
        }
        val = json_object_dotget_value(conf_obj, "lbt_cfg.tx_delay_2ch_us"); /* fetch value (if possible) */
        if (json_value_get_type(val) == JSONNumber) {
            lbtconf.tx_delay_2ch_us = (uint32_t)json_value_get_number(val);
        } else {
            MSG("WARNING: Data type for lbt_cfg.tx_delay_2ch_us seems wrong, please check\n");
            lbtconf.tx_delay_2ch_us = 0;
        }
    }
    /* all parameters parsed, submitting configuration to the HAL */
    if (lgw_lbt_setconf(lbtconf) != LGW_HAL_SUCCESS) {
        MSG("WARNING: Failed to configure lbt\n");
    }

    /* set antenna gain configuration */
    val = json_object_get_value(conf_obj, "antenna_gain"); /* fetch value (if possible) */
    if (json_value_get_type(val) == JSONNumber) {
        antenna_gain = (int8_t)json_value_get_number(val);
    } else {
        MSG("WARNING: Data type for antenna_gain seems wrong, please check\n");
        antenna_gain = 0;
    }
    MSG("INFO: antenna_gain %d dBi\n", antenna_gain);

    /* set configuration for tx gains */
    memset(&txlut, 0, sizeof txlut); /* initialize configuration structure */
    for (i = 0; i < TX_GAIN_LUT_SIZE_MAX; i++) {
        snprintf(param_name, sizeof param_name, "tx_lut_%i", i); /* compose parameter path inside JSON structure */
        val = json_object_get_value(conf_obj, param_name); /* fetch value (if possible) */
        if (json_value_get_type(val) != JSONObject) {
            MSG("INFO: no configuration for tx gain lut %i\n", i);
            continue;
        }
        txlut.size++; /* update TX LUT size based on JSON object found in configuration file */
        /* there is an object to configure that TX gain index, let's parse it */
        snprintf(param_name, sizeof param_name, "tx_lut_%i.pa_gain", i);
        val = json_object_dotget_value(conf_obj, param_name);
        if (json_value_get_type(val) == JSONNumber) {
            txlut.lut[i].pa_gain = (uint8_t)json_value_get_number(val);
        } else {
            MSG("WARNING: Data type for %s[%d] seems wrong, please check\n", param_name, i);
            txlut.lut[i].pa_gain = 0;
        }
                snprintf(param_name, sizeof param_name, "tx_lut_%i.dac_gain", i);
                val = json_object_dotget_value(conf_obj, param_name);
                if (json_value_get_type(val) == JSONNumber) {
                        txlut.lut[i].dac_gain = (uint8_t)json_value_get_number(val);
                } else {
                        txlut.lut[i].dac_gain = 3; /* This is the only dac_gain supported for now */
                }
                snprintf(param_name, sizeof param_name, "tx_lut_%i.dig_gain", i);
                val = json_object_dotget_value(conf_obj, param_name);
                if (json_value_get_type(val) == JSONNumber) {
                        txlut.lut[i].dig_gain = (uint8_t)json_value_get_number(val);
                } else {
            MSG("WARNING: Data type for %s[%d] seems wrong, please check\n", param_name, i);
                        txlut.lut[i].dig_gain = 0;
                }
                snprintf(param_name, sizeof param_name, "tx_lut_%i.mix_gain", i);
                val = json_object_dotget_value(conf_obj, param_name);
                if (json_value_get_type(val) == JSONNumber) {
                        txlut.lut[i].mix_gain = (uint8_t)json_value_get_number(val);
                } else {
            MSG("WARNING: Data type for %s[%d] seems wrong, please check\n", param_name, i);
                        txlut.lut[i].mix_gain = 0;
                }
                snprintf(param_name, sizeof param_name, "tx_lut_%i.rf_power", i);
                val = json_object_dotget_value(conf_obj, param_name);
                if (json_value_get_type(val) == JSONNumber) {
                        txlut.lut[i].rf_power = (int8_t)json_value_get_number(val);
                } else {
            MSG("WARNING: Data type for %s[%d] seems wrong, please check\n", param_name, i);
                        txlut.lut[i].rf_power = 0;
                }
    }
    /* all parameters parsed, submitting configuration to the HAL */
    MSG("INFO: Configuring TX LUT with %u indexes\n", txlut.size);
        if (lgw_txgain_setconf(&txlut) != LGW_HAL_SUCCESS) {
                MSG("WARNING: Failed to configure concentrator TX Gain LUT\n");
    }

    /* set configuration for RF chains */
    for (i = 0; i < LGW_RF_CHAIN_NB; ++i) {
        memset(&rfconf, 0, sizeof rfconf); /* initialize configuration structure */
        snprintf(param_name, sizeof param_name, "radio_%i", i); /* compose parameter path inside JSON structure */
        val = json_object_get_value(conf_obj, param_name); /* fetch value (if possible) */
        if (json_value_get_type(val) != JSONObject) {
            MSG("INFO: no configuration for radio %i\n", i);
            continue;
        }
        /* there is an object to configure that radio, let's parse it */
        snprintf(param_name, sizeof param_name, "radio_%i.enable", i);
        val = json_object_dotget_value(conf_obj, param_name);
        if (json_value_get_type(val) == JSONBoolean) {
            rfconf.enable = (bool)json_value_get_boolean(val);
        } else {
            rfconf.enable = false;
        }
        if (rfconf.enable == false) { /* radio disabled, nothing else to parse */
            MSG("INFO: radio %i disabled\n", i);
        } else  { /* radio enabled, will parse the other parameters */
            snprintf(param_name, sizeof param_name, "radio_%i.freq", i);
            rfconf.freq_hz = (uint32_t)json_object_dotget_number(conf_obj, param_name);
            snprintf(param_name, sizeof param_name, "radio_%i.rssi_offset", i);
            rfconf.rssi_offset = (float)json_object_dotget_number(conf_obj, param_name);
            snprintf(param_name, sizeof param_name, "radio_%i.type", i);
            str = json_object_dotget_string(conf_obj, param_name);
            if (!strncmp(str, "SX1255", 6)) {
                rfconf.type = LGW_RADIO_TYPE_SX1255;
            } else if (!strncmp(str, "SX1257", 6)) {
                rfconf.type = LGW_RADIO_TYPE_SX1257;
            } else {
                MSG("WARNING: invalid radio type: %s (should be SX1255 or SX1257)\n", str);
            }
            snprintf(param_name, sizeof param_name, "radio_%i.tx_enable", i);
            val = json_object_dotget_value(conf_obj, param_name);
            if (json_value_get_type(val) == JSONBoolean) {
                rfconf.tx_enable = (bool)json_value_get_boolean(val);
                if (rfconf.tx_enable == true) {
                    /* tx is enabled on this rf chain, we need its frequency range */
                    snprintf(param_name, sizeof param_name, "radio_%i.tx_freq_min", i);
                    tx_freq_min[i] = (uint32_t)json_object_dotget_number(conf_obj, param_name);
                    snprintf(param_name, sizeof param_name, "radio_%i.tx_freq_max", i);
                    tx_freq_max[i] = (uint32_t)json_object_dotget_number(conf_obj, param_name);
                    if ((tx_freq_min[i] == 0) || (tx_freq_max[i] == 0)) {
                        MSG("WARNING: no frequency range specified for TX rf chain %d\n", i);
                    }
                }
            } else {
                rfconf.tx_enable = false;
            }
            MSG("INFO: radio %i enabled (type %s), center frequency %u, RSSI offset %f, tx enabled %d\n", i, str, rfconf.freq_hz, rfconf.rssi_offset, rfconf.tx_enable);
        }
        /* all parameters parsed, submitting configuration to the HAL */
        if (lgw_rxrf_setconf(i, rfconf) != LGW_HAL_SUCCESS) {
            MSG("WARNING: invalid configuration for radio %i\n", i);
        }
    }

    /* set configuration for Lora multi-SF channels (bandwidth cannot be set) */
    for (i = 0; i < LGW_MULTI_NB; ++i) {
        memset(&ifconf, 0, sizeof ifconf); /* initialize configuration structure */
        snprintf(param_name, sizeof param_name, "chan_multiSF_%i", i); /* compose parameter path inside JSON structure */
        val = json_object_get_value(conf_obj, param_name); /* fetch value (if possible) */
        if (json_value_get_type(val) != JSONObject) {
            MSG("INFO: no configuration for Lora multi-SF channel %i\n", i);
            continue;
        }
        /* there is an object to configure that Lora multi-SF channel, let's parse it */
        snprintf(param_name, sizeof param_name, "chan_multiSF_%i.enable", i);
        val = json_object_dotget_value(conf_obj, param_name);
        if (json_value_get_type(val) == JSONBoolean) {
            ifconf.enable = (bool)json_value_get_boolean(val);
        } else {
            ifconf.enable = false;
        }
        if (ifconf.enable == false) { /* Lora multi-SF channel disabled, nothing else to parse */
            MSG("INFO: Lora multi-SF channel %i disabled\n", i);
        } else  { /* Lora multi-SF channel enabled, will parse the other parameters */
            snprintf(param_name, sizeof param_name, "chan_multiSF_%i.radio", i);
            ifconf.rf_chain = (uint32_t)json_object_dotget_number(conf_obj, param_name);
            snprintf(param_name, sizeof param_name, "chan_multiSF_%i.if", i);
            ifconf.freq_hz = (int32_t)json_object_dotget_number(conf_obj, param_name);
            // TODO: handle individual SF enabling and disabling (spread_factor)
            MSG("INFO: Lora multi-SF channel %i>  radio %i, IF %i Hz, 125 kHz bw, SF 7 to 12\n", i, ifconf.rf_chain, ifconf.freq_hz);
        }
        /* all parameters parsed, submitting configuration to the HAL */
        if (lgw_rxif_setconf(i, ifconf) != LGW_HAL_SUCCESS) {
            MSG("WARNING: invalid configuration for Lora multi-SF channel %i\n", i);
        }
    }

    /* set configuration for Lora standard channel */
    memset(&ifconf, 0, sizeof ifconf); /* initialize configuration structure */
    val = json_object_get_value(conf_obj, "chan_Lora_std"); /* fetch value (if possible) */
    if (json_value_get_type(val) != JSONObject) {
        MSG("INFO: no configuration for Lora standard channel\n");
    } else {
        val = json_object_dotget_value(conf_obj, "chan_Lora_std.enable");
        if (json_value_get_type(val) == JSONBoolean) {
            ifconf.enable = (bool)json_value_get_boolean(val);
        } else {
            ifconf.enable = false;
        }
        if (ifconf.enable == false) {
            MSG("INFO: Lora standard channel %i disabled\n", i);
        } else  {
            ifconf.rf_chain = (uint32_t)json_object_dotget_number(conf_obj, "chan_Lora_std.radio");
            ifconf.freq_hz = (int32_t)json_object_dotget_number(conf_obj, "chan_Lora_std.if");
            bw = (uint32_t)json_object_dotget_number(conf_obj, "chan_Lora_std.bandwidth");
            switch(bw) {
                case 500000: ifconf.bandwidth = BW_500KHZ; break;
                case 250000: ifconf.bandwidth = BW_250KHZ; break;
                case 125000: ifconf.bandwidth = BW_125KHZ; break;
                default: ifconf.bandwidth = BW_UNDEFINED;
            }
            sf = (uint32_t)json_object_dotget_number(conf_obj, "chan_Lora_std.spread_factor");
            switch(sf) {
                case  7: ifconf.datarate = DR_LORA_SF7;  break;
                case  8: ifconf.datarate = DR_LORA_SF8;  break;
                case  9: ifconf.datarate = DR_LORA_SF9;  break;
                case 10: ifconf.datarate = DR_LORA_SF10; break;
                case 11: ifconf.datarate = DR_LORA_SF11; break;
                case 12: ifconf.datarate = DR_LORA_SF12; break;
                default: ifconf.datarate = DR_UNDEFINED;
            }
            MSG("INFO: Lora std channel> radio %i, IF %i Hz, %u Hz bw, SF %u\n", ifconf.rf_chain, ifconf.freq_hz, bw, sf);
        }
        if (lgw_rxif_setconf(8, ifconf) != LGW_HAL_SUCCESS) {
            MSG("WARNING: invalid configuration for Lora standard channel\n");
        }
    }

    /* set configuration for FSK channel */
    memset(&ifconf, 0, sizeof ifconf); /* initialize configuration structure */
    val = json_object_get_value(conf_obj, "chan_FSK"); /* fetch value (if possible) */
    if (json_value_get_type(val) != JSONObject) {
        MSG("INFO: no configuration for FSK channel\n");
    } else {
        val = json_object_dotget_value(conf_obj, "chan_FSK.enable");
        if (json_value_get_type(val) == JSONBoolean) {
            ifconf.enable = (bool)json_value_get_boolean(val);
        } else {
            ifconf.enable = false;
        }
        if (ifconf.enable == false) {
            MSG("INFO: FSK channel %i disabled\n", i);
        } else  {
            ifconf.rf_chain = (uint32_t)json_object_dotget_number(conf_obj, "chan_FSK.radio");
            ifconf.freq_hz = (int32_t)json_object_dotget_number(conf_obj, "chan_FSK.if");
            bw = (uint32_t)json_object_dotget_number(conf_obj, "chan_FSK.bandwidth");
            fdev = (uint32_t)json_object_dotget_number(conf_obj, "chan_FSK.freq_deviation");
            ifconf.datarate = (uint32_t)json_object_dotget_number(conf_obj, "chan_FSK.datarate");

            /* if chan_FSK.bandwidth is set, it has priority over chan_FSK.freq_deviation */
            if ((bw == 0) && (fdev != 0)) {
                bw = 2 * fdev + ifconf.datarate;
            }
            if      (bw == 0)      ifconf.bandwidth = BW_UNDEFINED;
            else if (bw <= 7800)   ifconf.bandwidth = BW_7K8HZ;
            else if (bw <= 15600)  ifconf.bandwidth = BW_15K6HZ;
            else if (bw <= 31200)  ifconf.bandwidth = BW_31K2HZ;
            else if (bw <= 62500)  ifconf.bandwidth = BW_62K5HZ;
            else if (bw <= 125000) ifconf.bandwidth = BW_125KHZ;
            else if (bw <= 250000) ifconf.bandwidth = BW_250KHZ;
            else if (bw <= 500000) ifconf.bandwidth = BW_500KHZ;
            else ifconf.bandwidth = BW_UNDEFINED;

            MSG("INFO: FSK channel> radio %i, IF %i Hz, %u Hz bw, %u bps datarate\n", ifconf.rf_chain, ifconf.freq_hz, bw, ifconf.datarate);
        }
        if (lgw_rxif_setconf(9, ifconf) != LGW_HAL_SUCCESS) {
            MSG("WARNING: invalid configuration for FSK channel\n");
        }
    }
    json_value_free(root_val);
    return 0;
}

static int parse_gateway_configuration(const char * conf_file) {
    const char conf_obj_name[] = "gateway_conf";
    JSON_Value *root_val;
    JSON_Object *conf_obj = NULL;
    JSON_Value *val = NULL; /* needed to detect the absence of some fields */
	JSON_Value *val1 = NULL; /* needed to detect the absence of some fields */
	JSON_Value *val2 = NULL; /* needed to detect the absence of some fields */
	JSON_Array *servers = NULL;
	JSON_Array *syscalls = NULL;
    const char *str; /* pointer to sub-strings in the JSON data */
    unsigned long long ull = 0;
	int i; /* Loop variable */
	int ic; /* Server counter */

    /* try to parse JSON */
    root_val = json_parse_file_with_comments(conf_file);
    if (root_val == NULL) {
        MSG("ERROR: %s is not a valid JSON file\n", conf_file);
        exit(EXIT_FAILURE);
    }

    /* point to the gateway configuration object */
    conf_obj = json_object_get_object(json_value_get_object(root_val), conf_obj_name);
    if (conf_obj == NULL) {
        MSG("INFO: %s does not contain a JSON object named %s\n", conf_file, conf_obj_name);
        return -1;
    } else {
        MSG("INFO: %s does contain a JSON object named %s, parsing gateway parameters\n", conf_file, conf_obj_name);
    }

    /* gateway unique identifier (aka MAC address) (optional) */
    str = json_object_get_string(conf_obj, "gateway_ID");
    if (str != NULL) {
    	strncpy(gateway_id, str, sizeof gateway_id);
        sscanf(str, "%llx", &ull);
        lgwm = ull;
        MSG("INFO: gateway MAC address is configured to %016llX\n", ull);
    }

	/* Obtain multiple servers hostnames and ports from array */
	JSON_Object *nw_server = NULL;
	servers = json_object_get_array(conf_obj, "servers");
	if (servers != NULL) {
		/* serv_count represents the maximal number of servers to be read. */
		serv_count = json_array_get_count(servers);
		MSG("INFO: Found %i servers in array.\n", serv_count);
		ic = 0;
		for (i = 0; i < serv_count  && ic < MAX_SERVERS; i++) {
			nw_server = json_array_get_object(servers,i);
			str = json_object_get_string(nw_server, "server_address");
			val = json_object_get_value(nw_server, "serv_enabled");
			val1 = json_object_get_value(nw_server, "serv_port_up");
			val2 = json_object_get_value(nw_server, "serv_port_down");
			/* Try to read the fields */
			if (str != NULL)  strncpy(serv_addr[ic], str, sizeof serv_addr[ic]);
			if (val1 != NULL) snprintf(serv_port_up[ic], sizeof serv_port_up[ic], "%u", (uint16_t)json_value_get_number(val1));
			if (val2 != NULL) snprintf(serv_port_down[ic], sizeof serv_port_down[ic], "%u", (uint16_t)json_value_get_number(val2));
			/* If there is no server name we can only silently progress to the next entry */
			if (str == NULL) {
				continue;
			}
			/* If there are no ports report and progress to the next entry */
			else if ((val1 == NULL) || (val2 == NULL)) {
				MSG("INFO: Skipping server \"%s\" with at least one invalid port number\n", serv_addr[ic]);
				continue;
			}
            /* If the server was explicitly disabled, report and progress to the next entry */
			else if ( (val != NULL) && ((json_value_get_type(val)) == JSONBoolean) && ((bool)json_value_get_boolean(val) == false )) {
				MSG("INFO: Skipping disabled server \"%s\"\n", serv_addr[ic]);
				continue;
			}
			/* All test survived, this is a valid server, report and increase server counter. */
			MSG("INFO: Server %i configured to \"%s\", with port up \"%s\" and port down \"%s\"\n", ic, serv_addr[ic],serv_port_up[ic],serv_port_down[ic]);
			/* The server may be valid, it is not yet live. */
			serv_live[ic] = false;
			ic++;
		}
		serv_count = ic;
	} else {
		/* If there are no servers in server array fall back to old fashioned single server definition.
		 * The difference with the original situation is that we require a complete definition. */
    /* server hostname or IP address (optional) */
    str = json_object_get_string(conf_obj, "server_address");
		val1 = json_object_get_value(conf_obj, "serv_port_up");
		val2 = json_object_get_value(conf_obj, "serv_port_down");
		if ((str != NULL) && (val1 != NULL) && (val2 != NULL)) {
			serv_count = 1;
			serv_live[0] = false;
			strncpy(serv_addr[0], str, sizeof serv_addr[0]);
			snprintf(serv_port_up[0], sizeof serv_port_up[0], "%u", (uint16_t)json_value_get_number(val1));
			snprintf(serv_port_down[0], sizeof serv_port_down[0], "%u", (uint16_t)json_value_get_number(val2));
			MSG("INFO: Server configured to \"%s\", with port up \"%s\" and port down \"%s\"\n", serv_addr[0],serv_port_up[0],serv_port_down[0]);
		}
	}


	/* Using the defaults in case no values are present in the JSON */
	//TODO: Eliminate this default behavior, the server should be well configured or stop.
	if (serv_count == 0) {
		MSG("INFO: Using defaults for server and ports (specific ports are ignored if no server is defined)");
		strncpy(serv_addr[0],STR(DEFAULT_SERVER),sizeof(STR(DEFAULT_SERVER)));
		strncpy(serv_port_up[0],STR(DEFAULT_PORT_UP),sizeof(STR(DEFAULT_PORT_UP)));
		strncpy(serv_port_down[0],STR(DEFAULT_PORT_DW),sizeof(STR(DEFAULT_PORT_DW)));
		serv_live[0] = false;
		serv_count = 1;
	}

	/* Read the system calls for the monitor function. */
	syscalls = json_object_get_array(conf_obj, "system_calls");
	if (syscalls != NULL) {
		/* serv_count represents the maximal number of servers to be read. */
		mntr_sys_count = json_array_get_count(syscalls);
		MSG("INFO: Found %i system calls in array.\n", mntr_sys_count);
		for (i = 0; i < mntr_sys_count  && i < MNTR_SYS_MAX; i++) {
			str = json_array_get_string(syscalls,i);
			strncpy(mntr_sys_list[i], str, sizeof mntr_sys_list[i]);
			MSG("INFO: System command %i: \"%s\"\n",i,mntr_sys_list[i]);
		}
	}

	/* monitor hostname or IP address (optional) */
	str = json_object_get_string(conf_obj, "monitor_address");
    if (str != NULL) {
		strncpy(monitor_addr, str, sizeof monitor_addr);
		MSG("INFO: monitor hostname or IP address is configured to \"%s\"\n", monitor_addr);
    }

	/* get monitor connection port (optional) */
	val = json_object_get_value(conf_obj, "monitor_port");
    if (val != NULL) {
		snprintf(monitor_port, sizeof monitor_port, "%u", (uint16_t)json_value_get_number(val));
		MSG("INFO: monitor port is configured to \"%s\"\n", monitor_port);
    }

	/* ghost hostname or IP address (optional) */
	str = json_object_get_string(conf_obj, "ghost_address");
	if (str != NULL) {
		strncpy(ghost_addr, str, sizeof ghost_addr);
		MSG("INFO: ghost hostname or IP address is configured to \"%s\"\n", ghost_addr);
	}

	/* get ghost connection port (optional) */
	val = json_object_get_value(conf_obj, "ghost_port");
    if (val != NULL) {
		snprintf(ghost_port, sizeof ghost_port, "%u", (uint16_t)json_value_get_number(val));
		MSG("INFO: ghost port is configured to \"%s\"\n", ghost_port);
    }

	/* name of format, currently recognized are semtech and lorank (optional) */
	str = json_object_get_string(conf_obj, "stat_format");
	if (str != NULL) {
		strncpy(stat_format, str, sizeof stat_format);
		MSG("INFO: format is configured to \"%s\"\n", stat_format);
	}

	/* name of file to write statistical info to (optional) */
	str = json_object_get_string(conf_obj, "stat_file");
	if (str != NULL) {
		strncpy(stat_file, str, sizeof stat_file);
		MSG("INFO: filename for statistical performance is configured to \"%s\"\n", stat_file);
	}

	/* get percentage for dampening filter (optional) */
	val = json_object_get_value(conf_obj, "stat_damping");
    if (val != NULL) {
    	stat_damping = (int) json_value_get_number(val);
    	stat_damping = (stat_damping <= 0) ? 1 : (stat_damping >= 100) ? 99 : stat_damping;
		MSG("INFO: Damping for statistical info is configured to  %u%%\n", stat_damping);
    }

    /* get keep-alive interval (in seconds) for downstream (optional) */
    val = json_object_get_value(conf_obj, "keepalive_interval");
    if (val != NULL) {
        keepalive_time = (int)json_value_get_number(val);
        MSG("INFO: downstream keep-alive interval is configured to %u seconds\n", keepalive_time);
    }

    /* get interval (in seconds) for statistics display (optional) */
    val = json_object_get_value(conf_obj, "stat_interval");
    if (val != NULL) {
        stat_interval = (unsigned)json_value_get_number(val);
        MSG("INFO: statistics display interval is configured to %u seconds\n", stat_interval);
    }

    /* get time-out value (in ms) for upstream datagrams (optional) */
    val = json_object_get_value(conf_obj, "push_timeout_ms");
    if (val != NULL) {
        push_timeout_half.tv_usec = 500 * (long int)json_value_get_number(val);
        MSG("INFO: upstream PUSH_DATA time-out is configured to %u ms\n", (unsigned)(push_timeout_half.tv_usec / 500));
    }

    /* packet filtering parameters */
    val = json_object_get_value(conf_obj, "forward_crc_valid");
    if (json_value_get_type(val) == JSONBoolean) {
        fwd_valid_pkt = (bool)json_value_get_boolean(val);
    }
    MSG("INFO: packets received with a valid CRC will%s be forwarded\n", (fwd_valid_pkt ? "" : " NOT"));
    val = json_object_get_value(conf_obj, "forward_crc_error");
    if (json_value_get_type(val) == JSONBoolean) {
        fwd_error_pkt = (bool)json_value_get_boolean(val);
    }
    MSG("INFO: packets received with a CRC error will%s be forwarded\n", (fwd_error_pkt ? "" : " NOT"));
    val = json_object_get_value(conf_obj, "forward_crc_disabled");
    if (json_value_get_type(val) == JSONBoolean) {
        fwd_nocrc_pkt = (bool)json_value_get_boolean(val);
    }
    MSG("INFO: packets received with no CRC will%s be forwarded\n", (fwd_nocrc_pkt ? "" : " NOT"));

    /* GPS module TTY path (optional) */
    str = json_object_get_string(conf_obj, "gps_tty_path");
    if (str != NULL) {
        strncpy(gps_tty_path, str, sizeof gps_tty_path);
        MSG("INFO: GPS serial port path is configured to \"%s\"\n", gps_tty_path);
    }

	/* SSH path (optional) */
	str = json_object_get_string(conf_obj, "ssh_path");
	if (str != NULL) {
		strncpy(ssh_path, str, sizeof ssh_path);
		MSG("INFO: SSH path is configured to \"%s\"\n", ssh_path);
	}

	/* SSH port (optional) */
	val = json_object_get_value(conf_obj, "ssh_port");
	if (val != NULL) {
		ssh_port = (uint16_t) json_value_get_number(val);
		MSG("INFO: SSH port is configured to %u\n", ssh_port);
	}

	/* WEB port (optional) */
	val = json_object_get_value(conf_obj, "http_port");
	if (val != NULL) {
		http_port = (uint16_t) json_value_get_number(val);
		MSG("INFO: HTTP port is configured to %u\n", http_port);
	}

	/* NGROK path (optional) */
	str = json_object_get_string(conf_obj, "ngrok_path");
	if (str != NULL) {
		strncpy(ngrok_path, str, sizeof ngrok_path);
		MSG("INFO: NGROK path is configured to \"%s\"\n", ngrok_path);
	}

    /* get reference coordinates */
    val = json_object_get_value(conf_obj, "ref_latitude");
    if (val != NULL) {
        reference_coord.lat = (double)json_value_get_number(val);
        MSG("INFO: Reference latitude is configured to %f deg\n", reference_coord.lat);
    }
    val = json_object_get_value(conf_obj, "ref_longitude");
    if (val != NULL) {
        reference_coord.lon = (double)json_value_get_number(val);
        MSG("INFO: Reference longitude is configured to %f deg\n", reference_coord.lon);
    }
    val = json_object_get_value(conf_obj, "ref_altitude");
    if (val != NULL) {
        reference_coord.alt = (short)json_value_get_number(val);
        MSG("INFO: Reference altitude is configured to %i meters\n", reference_coord.alt);
    }

	/* Read the value for gps_enabled data */
	val = json_object_get_value(conf_obj, "gps");
	if (json_value_get_type(val) == JSONBoolean) {
		gps_enabled = (bool)json_value_get_boolean(val);
	}
	if (gps_enabled == true) {
		MSG("INFO: GPS is enabled\n");
	} else {
		MSG("INFO: GPS is disabled\n");
    }

	if (gps_enabled == true) {
    /* Gateway GPS coordinates hardcoding (aka. faking) option */
    val = json_object_get_value(conf_obj, "fake_gps");
    if (json_value_get_type(val) == JSONBoolean) {
        gps_fake_enable = (bool)json_value_get_boolean(val);
        if (gps_fake_enable == true) {
				MSG("INFO: Using fake GPS coordinates instead of real.\n");
        } else {
				MSG("INFO: Using real GPS if available.\n");
        }
    }
	}

    /* Beacon signal period (optional) */
    val = json_object_get_value(conf_obj, "beacon_period");
    if (val != NULL) {
        beacon_period = (uint32_t)json_value_get_number(val);
        MSG("INFO: Beaconing period is configured to %u seconds\n", beacon_period);
    }

    /* Beacon TX frequency (optional) */
    val = json_object_get_value(conf_obj, "beacon_freq_hz");
    if (val != NULL) {
        beacon_freq_hz = (uint32_t)json_value_get_number(val);
        MSG("INFO: Beaconing signal will be emitted at %u Hz\n", beacon_freq_hz);
    }

	/* Read the value for upstream data */
	val = json_object_get_value(conf_obj, "upstream");
	if (json_value_get_type(val) == JSONBoolean) {
		upstream_enabled = (bool)json_value_get_boolean(val);
	}
	if (upstream_enabled == true) {
		MSG("INFO: Upstream data is enabled\n");
	} else {
		MSG("INFO: Upstream data is disabled\n");
	}

	/* Read the value for downstream_enabled data */
	val = json_object_get_value(conf_obj, "downstream");
	if (json_value_get_type(val) == JSONBoolean) {
		downstream_enabled = (bool)json_value_get_boolean(val);
	}
	if (downstream_enabled == true) {
		MSG("INFO: Downstream data is enabled\n");
	} else {
		MSG("INFO: Downstream data is disabled\n");
	}

	/* Read the value for ghoststream_enabled data */
	val = json_object_get_value(conf_obj, "ghoststream");
	if (json_value_get_type(val) == JSONBoolean) {
		ghoststream_enabled = (bool)json_value_get_boolean(val);
	}
	if (ghoststream_enabled == true) {
		MSG("INFO: Ghoststream data is enabled\n");
	} else {
		MSG("INFO: Ghoststream data is disabled\n");
	}

	/* Read the value for radiostream_enabled data */
	val = json_object_get_value(conf_obj, "radiostream");
	if (json_value_get_type(val) == JSONBoolean) {
		radiostream_enabled = (bool)json_value_get_boolean(val);
	}
	if (radiostream_enabled == true) {
		MSG("INFO: Radiostream data is enabled\n");
	} else {
		MSG("INFO: Radiostream data is disabled\n");
    }

	/* Read the value for statusstream_enabled data */
	val = json_object_get_value(conf_obj, "statusstream");
	if (json_value_get_type(val) == JSONBoolean) {
		statusstream_enabled = (bool)json_value_get_boolean(val);
	}
	if (statusstream_enabled == true) {
		MSG("INFO: Statusstream data is enabled\n");
	} else {
		MSG("INFO: Statusstream data is disabled\n");
    }

	/* Read the value for beacon_enabled data */
	val = json_object_get_value(conf_obj, "beacon");
	if (json_value_get_type(val) == JSONBoolean) {
		beacon_enabled = (bool)json_value_get_boolean(val);
	}
	if (beacon_enabled == true) {
		MSG("INFO: Beacon is enabled\n");
	} else {
		MSG("INFO: Beacon is disabled\n");
    }

	/* Read the value for monitor_enabled data */
	val = json_object_get_value(conf_obj, "monitor");
	if (json_value_get_type(val) == JSONBoolean) {
		monitor_enabled = (bool)json_value_get_boolean(val);
	}
	if (monitor_enabled == true) {
		MSG("INFO: Monitor is enabled\n");
	} else {
		MSG("INFO: Monitor is disabled\n");
    }

	/* Read the value for logger_enabled data */
	val = json_object_get_value(conf_obj, "logger");
	if (json_value_get_type(val) == JSONBoolean) {
		logger_enabled = (bool)json_value_get_boolean(val);
	}
	if (logger_enabled == true) {
		MSG("INFO: Packet logger is enabled\n");
	} else {
		MSG("INFO: Packet logger is disabled\n");
    }

	/* Auto-quit threshold (optional) */
    val = json_object_get_value(conf_obj, "autoquit_threshold");
    if (val != NULL) {
        autoquit_threshold = (uint32_t)json_value_get_number(val);
        MSG("INFO: Auto-quit after %u non-acknowledged PULL_DATA\n", autoquit_threshold);
    }

	/* Platform read and override */
	str = json_object_get_string(conf_obj, "platform");
	if (str != NULL) {
		if (strncmp(str, "*", 1) != 0) { strncpy(platform, str, sizeof platform); }
		MSG("INFO: Platform configured to \"%s\"\n", platform);
	}

	/* Read of contact email */
	str = json_object_get_string(conf_obj, "contact_email");
	if (str != NULL) {
		strncpy(email, str, sizeof email);
		MSG("INFO: Contact email configured to \"%s\"\n", email);
	}

	/* Read of description */
	str = json_object_get_string(conf_obj, "description");
	if (str != NULL) {
		strncpy(description, str, sizeof description);
		MSG("INFO: Description configured to \"%s\"\n", description);
	}

    /* free JSON parsing data structure */
    json_value_free(root_val);
    return 0;
}

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

#if 0
static uint8_t crc8_ccit(const uint8_t * data, unsigned size) {
    const uint8_t crc_poly = 0x87; /* CCITT */
    const uint8_t init_val = 0xFF; /* CCITT */
    uint8_t x = init_val;
    unsigned i, j;

    if (data == NULL)  {
        return 0;
    }

    for (i=0; i<size; ++i) {
        x ^= data[i];
        for (j=0; j<8; ++j) {
            x = (x & 0x80) ? (x<<1) ^ crc_poly : (x<<1);
        }
    }

    return x;
}
#endif

double difftimespec(struct timespec end, struct timespec beginning) {
    double x;

    x = 1E-9 * (double)(end.tv_nsec - beginning.tv_nsec);
    x += (double)(end.tv_sec - beginning.tv_sec);

    return x;
}

/* Function to safely calculate the moving averages */
static double moveave(double old, uint32_t utel, uint32_t unoe) {
	double dtel = utel;
	double dnoe = unoe;
	if      ( unoe == 0 )   { return old; }
	else if ( old  < 1e-3 ) { return dtel / dnoe; }
	else                    { return (old*stat_damping + (dtel/dnoe)*(100-stat_damping))/100; }
}

//TODO: Check if this is a propper generalization of servers!
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

/* -------------------------------------------------------------------------- */
/* --- MAIN FUNCTION -------------------------------------------------------- */

int main(void)
{
    struct sigaction sigact; /* SIGQUIT&SIGINT&SIGTERM signal handling */
    int i; /* loop variable and temporary variable for return value */
	int ic; /* Server loop variable */

    /* configuration file related */
    char *global_cfg_path= "global_conf.json"; /* contain global (typ. network-wide) configuration */
    char *local_cfg_path = "local_conf.json"; /* contain node specific configuration, overwrite global parameters for parameters that are defined in both */
    char *debug_cfg_path = "debug_conf.json"; /* if present, all other configuration files are ignored */

    /* threads */
    pthread_t thrid_up;
	pthread_t thrid_down[MAX_SERVERS];
    pthread_t thrid_gps;
    pthread_t thrid_valid;
    pthread_t thrid_jit;
    pthread_t thrid_timersync;

    /* network socket creation */
    struct addrinfo hints;
    struct addrinfo *result; /* store result of getaddrinfo */
    struct addrinfo *q; /* pointer to move into *result data */
    char host_name[64];
    char port_name[64];

    /* variables to get local copies of measurements */
    uint32_t cp_nb_rx_rcv = 0;
    uint32_t cp_nb_rx_ok = 0;
    uint32_t cp_nb_rx_bad = 0;
    uint32_t cp_nb_rx_drop = 0;
    uint32_t cp_nb_rx_nocrc = 0;
    uint32_t cp_up_pkt_fwd = 0;
    uint32_t cp_up_network_byte = 0;
    uint32_t cp_up_payload_byte = 0;
    uint32_t cp_up_dgram_sent = 0;
    uint32_t cp_up_ack_rcv = 0;
    uint32_t cp_dw_pull_sent = 0;
    uint32_t cp_dw_ack_rcv = 0;
    uint32_t cp_dw_dgram_rcv = 0;
    uint32_t cp_dw_dgram_acp = 0;
    uint32_t cp_dw_network_byte = 0;
    uint32_t cp_dw_payload_byte = 0;
    uint32_t cp_nb_tx_ok = 0;
    uint32_t cp_nb_tx_fail = 0;
    uint32_t cp_nb_tx_requested = 0;
    uint32_t cp_nb_tx_rejected_collision_packet = 0;
    uint32_t cp_nb_tx_rejected_collision_beacon = 0;
    uint32_t cp_nb_tx_rejected_too_late = 0;
    uint32_t cp_nb_tx_rejected_too_early = 0;
    uint32_t cp_nb_beacon_queued = 0;
    uint32_t cp_nb_beacon_sent = 0;
    uint32_t cp_nb_beacon_rejected = 0;

    /* array to collect data per server */
    int ar_up_dgram_sent[MAX_SERVERS] = {0};
    int ar_up_ack_rcv[MAX_SERVERS]    = {0};
    int ar_dw_pull_sent[MAX_SERVERS]  = {0};
    int ar_dw_ack_rcv[MAX_SERVERS]    = {0};
    int ar_dw_dgram_rcv[MAX_SERVERS]  = {0};
    int ar_dw_dgram_acp[MAX_SERVERS]  = {0};

    /* moving averages for overall statistics */
    double move_up_rx_quality                     =  0;   /* ratio of received crc_good packets over total received packets */
    double move_up_ack_quality[MAX_SERVERS]       = {0};  /* ratio of datagram sent to datagram acknowledged to server */
    double move_dw_ack_quality[MAX_SERVERS]       = {0};  /* ratio of pull request to pull response to server */
    double move_dw_datagram_quality[MAX_SERVERS]  = {0};  /* ratio of json correct datagrams to total datagrams received*/
    double move_dw_receive_quality[MAX_SERVERS]   = {0};  /* ratio of succesfully aired data packets to total received datapackets */
    double move_dw_beacon_quality                 =  0;   /* ratio of succesfully sent to queued for the beacon */

    /* GPS coordinates variables */
    bool coord_ok = false;
    struct coord_s cp_gps_coord = {0.0, 0.0, 0};
    //struct coord_s cp_gps_err;

    /* statistics variable */
    char * stat_file_tmp = ".temp_statistics_polypacketforwarder";
    char stat_timestamp[24];
    char iso_timestamp[24];
    float rx_ok_ratio;
    float rx_bad_ratio;
    float rx_nocrc_ratio;
    float up_ack_ratio;
    float dw_ack_ratio;

    /* fields to store the moment of activation. */
    time_t startup_time = time(NULL);
    time_t current_time = startup_time;

    /* display version informations */
	MSG("*** Poly Packet Forwarder for Lora Gateway ***\nVersion: " VERSION_STRING "\n");
    MSG("*** Lora concentrator HAL library version info ***\n%s\n***\n", lgw_version_info());

    /* display host endianness */
    #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        MSG("INFO: Little endian host\n");
    #elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        MSG("INFO: Big endian host\n");
    #else
        MSG("INFO: Host endianness unknown\n");
    #endif

    /* load configuration files */
    if (access(debug_cfg_path, R_OK) == 0) { /* if there is a debug conf, parse only the debug conf */
        MSG("INFO: found debug configuration file %s, parsing it\n", debug_cfg_path);
        MSG("INFO: other configuration files will be ignored\n");
        parse_SX1301_configuration(debug_cfg_path);
        parse_gateway_configuration(debug_cfg_path);
    } else if (access(global_cfg_path, R_OK) == 0) { /* if there is a global conf, parse it and then try to parse local conf  */
        MSG("INFO: found global configuration file %s, parsing it\n", global_cfg_path);
        parse_SX1301_configuration(global_cfg_path);
        parse_gateway_configuration(global_cfg_path);
        if (access(local_cfg_path, R_OK) == 0) {
            MSG("INFO: found local configuration file %s, parsing it\n", local_cfg_path);
            MSG("INFO: redefined parameters will overwrite global parameters\n");
            parse_SX1301_configuration(local_cfg_path);
            parse_gateway_configuration(local_cfg_path);
        }
    } else if (access(local_cfg_path, R_OK) == 0) { /* if there is only a local conf, parse it and that's all */
        MSG("INFO: found local configuration file %s, parsing it\n", local_cfg_path);
        parse_SX1301_configuration(local_cfg_path);
        parse_gateway_configuration(local_cfg_path);
    } else {
        MSG("ERROR: [main] failed to find any configuration file named %s, %s OR %s\n", global_cfg_path, local_cfg_path, debug_cfg_path);
        exit(EXIT_FAILURE);
    }

    /* Start GPS a.s.a.p., to allow it to lock */
	if (gps_enabled == true) {
		if ((gps_fake_enable == false) && (gps_tty_path[0] != '\0')) { /* do not try to open GPS device if no path set */
			i = lgw_gps_enable(gps_tty_path, "ubx7", 0, &gps_tty_fd); /* HAL only supports u-blox 7 for now */
        if (i != LGW_GPS_SUCCESS) {
            MSG("WARNING: [main] impossible to open %s for GPS sync (check permissions)\n", gps_tty_path);
				gps_active = false;
            gps_ref_valid = false;
        } else {
        	MSG("INFO: [main] TTY port %s open for GPS synchronization\n", gps_tty_path);
				gps_active = true;
            gps_ref_valid = false;
        }
		} else {
			gps_active = false;
			gps_ref_valid = false;
    }
	}

    /* get timezone info */
    tzset();

    /* sanity check on configuration variables */
    // TODO

    /* process some of the configuration variables */
    net_mac_h = htonl((uint32_t)(0xFFFFFFFF & (lgwm>>32)));
    net_mac_l = htonl((uint32_t)(0xFFFFFFFF &  lgwm  ));

    /* prepare hints to open network sockets */
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET; /* WA: Forcing IPv4 as AF_UNSPEC makes connection on localhost to fail */
    hints.ai_socktype = SOCK_DGRAM;

	/* Loop through all possible servers */
	for (ic = 0; ic < serv_count; ic++) {

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

	//TODO: Check if there are any live servers available, if not we should exit since there cannot be any
	// sensible course of action. Actually it would be best to redesign the whole communication loop, and take
	// the socket constructors to be inside a try-retry loop. That way we can respond to severs that implemented
	// there UDP handling erroneously, or any other temporal obstruction in the communication
	// path (broken stacks in routers for example) Now, contact may be lost for ever and a manual
	// restart at the this side is required.

    /* starting the concentrator */
	if (radiostream_enabled == true) {
		MSG("INFO: [main] Starting the concentrator\n");
    i = lgw_start();
    if (i == LGW_HAL_SUCCESS) {
			MSG("INFO: [main] concentrator started, radio packets can now be received.\n");
    } else {
        MSG("ERROR: [main] failed to start the concentrator\n");
        exit(EXIT_FAILURE);
    }
	} else {
		MSG("WARNING: Radio is disabled, radio packets cannot be send or received.\n");
	}

	
    /* spawn threads to manage upstream and downstream */
	if (upstream_enabled == true) {
    i = pthread_create( &thrid_up, NULL, (void * (*)(void *))thread_up, NULL);
    if (i != 0) {
        MSG("ERROR: [main] impossible to create upstream thread\n");
        exit(EXIT_FAILURE);
    }
	}
	if (downstream_enabled == true) {
		for (ic = 0; ic < serv_count; ic++) if (serv_live[ic] == true) {
			i = pthread_create( &thrid_down[ic], NULL, (void * (*)(void *))thread_down, (void *) (long) ic);
			if (i != 0) {
				MSG("ERROR: [main] impossible to create downstream thread\n");
				exit(EXIT_FAILURE);
			}
		}
	}

    i = pthread_create( &thrid_jit, NULL, (void * (*)(void *))thread_jit, NULL);
    if (i != 0) {
        MSG("ERROR: [main] impossible to create JIT thread\n");
        exit(EXIT_FAILURE);
    }

    // Timer synchronization does not make much sense without and active GPS becasue it never gets initialized ...
    if (gps_active == true) {
    	i = pthread_create( &thrid_timersync, NULL, (void * (*)(void *))thread_timersync, NULL);
    	if (i != 0) {
    		MSG("ERROR: [main] impossible to create Timer Sync thread\n");
    	exit(EXIT_FAILURE);
    	}
    }

    /* spawn thread to manage GPS */
	if (gps_active == true) {
        i = pthread_create( &thrid_gps, NULL, (void * (*)(void *))thread_gps, NULL);
        if (i != 0) {
            MSG("ERROR: [main] impossible to create GPS thread\n");
            exit(EXIT_FAILURE);
        }
        i = pthread_create( &thrid_valid, NULL, (void * (*)(void *))thread_valid, NULL);
        if (i != 0) {
            MSG("ERROR: [main] impossible to create validation thread\n");
            exit(EXIT_FAILURE);
        }
    }

    /* configure signal handling */
    sigemptyset(&sigact.sa_mask);
    sigact.sa_flags = 0;
    sigact.sa_handler = sig_handler;
    sigaction(SIGQUIT, &sigact, NULL); /* Ctrl-\ */
    sigaction(SIGINT, &sigact, NULL); /* Ctrl-C */
    sigaction(SIGTERM, &sigact, NULL); /* default "kill" command */

	/* Start the ghost Listener */
    if (ghoststream_enabled == true) {
    	ghost_start(ghost_addr,ghost_port,reference_coord,gateway_id);
		MSG("INFO: [main] Ghost listener started, ghost packets can now be received.\n");
    }
	
	/* Connect to the monitor server */
    if (monitor_enabled == true) {
    	monitor_start(monitor_addr,monitor_port);
		MSG("INFO: [main] Monitor contacted, monitor data can now be requested.\n");
    }

    /* Check if we have anything to do */
    if ( (radiostream_enabled == false) && (ghoststream_enabled == false) && (statusstream_enabled == false) && (monitor_enabled == false) ) {
    	MSG("WARNING: [main] All streams have been disabled, gateway may be completely silent.\n");
    }

    /* main loop task : statistics collection */
    while (!exit_sig && !quit_sig) {
        /* wait for next reporting interval */
        wait_ms(1000 * stat_interval);

        /* get timestamp for statistics */
        current_time = time(NULL);
        strftime(stat_timestamp, sizeof stat_timestamp, "%F %T %Z", gmtime(&current_time));
        strftime(iso_timestamp, sizeof stat_timestamp, "%FT%TZ", gmtime(&current_time));

        /* access upstream statistics, copy and reset them */
        pthread_mutex_lock(&mx_meas_up);
        cp_nb_rx_rcv       = meas_nb_rx_rcv;
        cp_nb_rx_ok        = meas_nb_rx_ok;
        cp_nb_rx_bad       = meas_nb_rx_bad;
        cp_nb_rx_nocrc     = meas_nb_rx_nocrc;
        cp_up_pkt_fwd      = meas_up_pkt_fwd;
        cp_up_network_byte = meas_up_network_byte;
        cp_up_payload_byte = meas_up_payload_byte;
        cp_nb_rx_drop      = cp_nb_rx_rcv - cp_nb_rx_ok - cp_nb_rx_bad - cp_nb_rx_nocrc;
        for (i=cp_up_dgram_sent=0; i<serv_count; i++) { cp_up_dgram_sent += ar_up_dgram_sent[i] = meas_up_dgram_sent[i]; }
        for (i=cp_up_ack_rcv=0;    i<serv_count; i++) { cp_up_ack_rcv    += ar_up_ack_rcv[i]    = meas_up_ack_rcv[i];    }
        meas_nb_rx_rcv = 0;
        meas_nb_rx_ok = 0;
        meas_nb_rx_bad = 0;
        meas_nb_rx_nocrc = 0;
        meas_up_pkt_fwd = 0;
        meas_up_network_byte = 0;
        meas_up_payload_byte = 0;
        memset(meas_up_dgram_sent, 0, sizeof meas_up_dgram_sent);
        memset(meas_up_ack_rcv, 0, sizeof meas_up_ack_rcv);
        pthread_mutex_unlock(&mx_meas_up);
        if (cp_nb_rx_rcv > 0) {
            rx_ok_ratio = (float)cp_nb_rx_ok / (float)cp_nb_rx_rcv;
            rx_bad_ratio = (float)cp_nb_rx_bad / (float)cp_nb_rx_rcv;
            rx_nocrc_ratio = (float)cp_nb_rx_nocrc / (float)cp_nb_rx_rcv;
        } else {
            rx_ok_ratio = 0.0;
            rx_bad_ratio = 0.0;
            rx_nocrc_ratio = 0.0;
        }
        if (cp_up_dgram_sent > 0) {
            up_ack_ratio = (float)cp_up_ack_rcv / (float)cp_up_dgram_sent;
        } else {
            up_ack_ratio = 0.0;
        }

        /* access downstream statistics, copy and reset them */
        pthread_mutex_lock(&mx_meas_dw);
        for (i=cp_dw_pull_sent=0; i<serv_count; i++) { cp_dw_pull_sent += ar_dw_pull_sent[i] = meas_dw_pull_sent[i]; }
        for (i=cp_dw_ack_rcv=0;   i<serv_count; i++) { cp_dw_ack_rcv   += ar_dw_ack_rcv[i]   = meas_dw_ack_rcv[i];   }
        for (i=cp_dw_dgram_rcv=0; i<serv_count; i++) { cp_dw_dgram_rcv += ar_dw_dgram_rcv[i] = meas_dw_dgram_rcv[i]; }
        for (i=cp_dw_dgram_acp=0; i<serv_count; i++) { cp_dw_dgram_acp += ar_dw_dgram_acp[i] = meas_dw_dgram_acp[i]; }
        cp_dw_network_byte =  meas_dw_network_byte;
        cp_dw_payload_byte =  meas_dw_payload_byte;
        cp_nb_tx_ok        =  meas_nb_tx_ok;
        cp_nb_tx_fail      =  meas_nb_tx_fail;
        //TODO: Why were here all '+=' instead of '='?? The summed values grow unbounded and eventually overflow!
        cp_nb_tx_requested                 =  meas_nb_tx_requested;                   // was +=
        cp_nb_tx_rejected_collision_packet =  meas_nb_tx_rejected_collision_packet;   // was +=
        cp_nb_tx_rejected_collision_beacon =  meas_nb_tx_rejected_collision_beacon;   // was +=
        cp_nb_tx_rejected_too_late         =  meas_nb_tx_rejected_too_late;           // was +=
        cp_nb_tx_rejected_too_early        =  meas_nb_tx_rejected_too_early;          // was +=
        cp_nb_beacon_queued   =  meas_nb_beacon_queued;    // was +=
        cp_nb_beacon_sent     =  meas_nb_beacon_sent;      // was +=
        cp_nb_beacon_rejected =  meas_nb_beacon_rejected;  // was +=
        memset(meas_dw_pull_sent, 0, sizeof meas_dw_pull_sent);
        memset(meas_dw_ack_rcv, 0, sizeof meas_dw_ack_rcv);
        memset(meas_dw_dgram_rcv, 0, sizeof meas_dw_dgram_rcv);
        memset(meas_dw_dgram_acp, 0, sizeof meas_dw_dgram_acp);
        meas_dw_network_byte = 0;
        meas_dw_payload_byte = 0;
        meas_nb_tx_ok = 0;
        meas_nb_tx_fail = 0;
        meas_nb_tx_requested = 0;
        meas_nb_tx_rejected_collision_packet = 0;
        meas_nb_tx_rejected_collision_beacon = 0;
        meas_nb_tx_rejected_too_late = 0;
        meas_nb_tx_rejected_too_early = 0;
        meas_nb_beacon_queued = 0;
        meas_nb_beacon_sent = 0;
        meas_nb_beacon_rejected = 0;
        pthread_mutex_unlock(&mx_meas_dw);
        if (cp_dw_pull_sent > 0) {
            dw_ack_ratio = (float)cp_dw_ack_rcv / (float)cp_dw_pull_sent;
        } else {
            dw_ack_ratio = 0.0;
        }

        /* access GPS statistics, copy them */
		if (gps_active == true) {
            pthread_mutex_lock(&mx_meas_gps);
            coord_ok = gps_coord_valid;
            cp_gps_coord  =  meas_gps_coord;
            //cp_gps_err    =  meas_gps_err;
            pthread_mutex_unlock(&mx_meas_gps);
        }

        /* overwrite with reference coordinates if function is enabled */
        if (gps_fake_enable == true) {
			//gps_enabled = true;
            coord_ok = true;
            cp_gps_coord = reference_coord;
        }

        /* calculate the moving averages */
        move_up_rx_quality =  moveave(move_up_rx_quality,cp_nb_rx_ok,cp_nb_rx_rcv);
		for (i=0; i<serv_count; i++) { move_up_ack_quality[i]      = moveave(move_up_ack_quality[i],ar_up_ack_rcv[i],ar_up_dgram_sent[i]); }
		for (i=0; i<serv_count; i++) { move_dw_ack_quality[i]      = moveave(move_dw_ack_quality[i],ar_dw_ack_rcv[i],ar_dw_pull_sent[i]); }
		for (i=0; i<serv_count; i++) { move_dw_datagram_quality[i] = moveave(move_dw_datagram_quality[i],ar_dw_dgram_acp[i],ar_dw_dgram_rcv[i]); }
		for (i=0; i<serv_count; i++) { move_dw_receive_quality[i]  = moveave(move_dw_receive_quality[i],ar_dw_ack_rcv[i],ar_dw_pull_sent[i]); }
		move_dw_beacon_quality =  moveave(move_dw_beacon_quality,cp_nb_beacon_sent,cp_nb_beacon_queued);

        /* display a report */
        printf("\n##### %s #####\n", stat_timestamp);
        if (upstream_enabled == true) {
        	printf("### [UPSTREAM] ###\n");
        	printf("# RF packets received by concentrator: %u\n", cp_nb_rx_rcv);
        	printf("# CRC_OK: %.2f%%, CRC_FAIL: %.2f%%, NO_CRC: %.2f%%\n", 100.0 * rx_ok_ratio, 100.0 * rx_bad_ratio, 100.0 * rx_nocrc_ratio);
        	printf("# RF packets forwarded: %u (%u bytes)\n", cp_up_pkt_fwd, cp_up_payload_byte);
        	printf("# PUSH_DATA datagrams sent: %u (%u bytes)\n", cp_up_dgram_sent, cp_up_network_byte);
        	printf("# PUSH_DATA acknowledged: %.2f%%\n", 100.0 * up_ack_ratio);
        } else {
        	printf("### UPSTREAM IS DISABLED! \n");
        }
        if (downstream_enabled == true) {
        	printf("### [DOWNSTREAM] ###\n");
        	printf("# PULL_DATA sent: %u (%.2f%% acknowledged)\n", cp_dw_pull_sent, 100.0 * dw_ack_ratio);
        	printf("# PULL_RESP(onse) datagrams received: %u (%u bytes)\n", cp_dw_dgram_rcv, cp_dw_network_byte);
        	printf("# RF packets sent to concentrator: %u (%u bytes)\n", (cp_nb_tx_ok+cp_nb_tx_fail), cp_dw_payload_byte);
        	printf("# TX errors: %u\n", cp_nb_tx_fail);
        	if (cp_nb_tx_requested != 0 ) {
        		printf("# TX rejected (collision packet): %.2f%% (req:%u, rej:%u)\n", 100.0 * cp_nb_tx_rejected_collision_packet / cp_nb_tx_requested, cp_nb_tx_requested, cp_nb_tx_rejected_collision_packet);
        		printf("# TX rejected (collision beacon): %.2f%% (req:%u, rej:%u)\n", 100.0 * cp_nb_tx_rejected_collision_beacon / cp_nb_tx_requested, cp_nb_tx_requested, cp_nb_tx_rejected_collision_beacon);
        		printf("# TX rejected (too late): %.2f%% (req:%u, rej:%u)\n", 100.0 * cp_nb_tx_rejected_too_late / cp_nb_tx_requested, cp_nb_tx_requested, cp_nb_tx_rejected_too_late);
        		printf("# TX rejected (too early): %.2f%% (req:%u, rej:%u)\n", 100.0 * cp_nb_tx_rejected_too_early / cp_nb_tx_requested, cp_nb_tx_requested, cp_nb_tx_rejected_too_early);
        	}
        } else {
        	printf("### DOWNSTREAM IS DISABLED! \n");
        }
        if (beacon_enabled == true) {
        	printf("### [BEACON] ###\n");
        	printf("# Packets queued: %u\n", cp_nb_beacon_queued);
        	printf("# Packets sent so far: %u\n", cp_nb_beacon_sent);
        	printf("# Packets rejected: %u\n", cp_nb_beacon_rejected);
        } else {
        	printf("### BEACON IS DISABLED! \n");
        }
     	printf("### [JIT] ###\n");
        jit_report_queue (&jit_queue);
		//TODO: this is not symmetrical. time can also be derived from other sources, fix
        if (gps_enabled == true) {
            printf("### [GPS] ###\n");
            /* no need for mutex, display is not critical */
            if (gps_fake_enable == true) {
				printf("# No time keeping possible due to fake gps.\n");
            } else if (gps_ref_valid == true) {
				printf("# Valid gps time reference (age: %li sec)\n", (long)difftime(time(NULL), time_reference_gps.systime));
            } else {
				printf("# Invalid gps time reference (age: %li sec)\n", (long)difftime(time(NULL), time_reference_gps.systime));
            }
            if (gps_fake_enable == true) {
				printf("# Manual GPS coordinates: latitude %.5f, longitude %.5f, altitude %i m\n", cp_gps_coord.lat, cp_gps_coord.lon, cp_gps_coord.alt);
            } else if (coord_ok == true) {
				printf("# System GPS coordinates: latitude %.5f, longitude %.5f, altitude %i m\n", cp_gps_coord.lat, cp_gps_coord.lon, cp_gps_coord.alt);
            } else {
                printf("# no valid GPS coordinates available yet\n");
            }
        } else {
            printf("### GPS IS DISABLED! \n");
        }

     	printf("### [PERFORMANCE] ###\n");
     	if (upstream_enabled == true) {
     		printf("# Upstream radio packet quality: %.2f%%.\n",100*move_up_rx_quality);
     		for (i=0; i<serv_count; i++) { printf("# Upstream datagram acknowledgment quality for server \"%s\" is %.2f%%.\n",serv_addr[i],100*move_up_ack_quality[i]); }
     	}
     	if (downstream_enabled == true) {
     		for (i=0; i<serv_count; i++) { printf("# Downstream heart beat acknowledgment quality for server \"%s\" is %.2f%%.\n",serv_addr[i],100*move_dw_ack_quality[i]); }
     		for (i=0; i<serv_count; i++) { printf("# Downstream datagram content quality for server \"%s\" is %.2f%%.\n",serv_addr[i],100*move_dw_datagram_quality[i]); }
     		for (i=0; i<serv_count; i++) { printf("# Downstream radio transmission quality for server \"%s\" is %.2f%%.\n",serv_addr[i],100*move_dw_receive_quality[i]); }
     	}
     	if (beacon_enabled == true) {
     		printf("# Downstream beacon transmission quality: %.2f%%.\n",100*move_dw_beacon_quality);
     	}


        /* generate a JSON report (will be sent to server by upstream thread) */

     	/* Check which format to use */
     	bool semtech_format = strcmp(stat_format,"semtech") == 0;
     	bool lorank_format  = strcmp(stat_format,"lorank")  == 0;
        JSON_Value *root_value = NULL;
        JSON_Object *root_object = NULL;
		if (statusstream_enabled == true) {
			if (lorank_format) {
	        root_value  = json_value_init_object();
	        root_object = json_value_get_object(root_value);
	        json_object_dotset_string(       root_object, "timestamp",                                    iso_timestamp);
	        json_object_dotset_string(       root_object, "device.id",                                    gateway_id);
	        json_object_dotset_number(       root_object, "device.latitude",                              cp_gps_coord.lat);
	        json_object_dotset_number(       root_object, "device.longitude",                             cp_gps_coord.lon);
	        json_object_dotset_number(       root_object, "device.altitude",                              cp_gps_coord.alt);
	        json_object_dotset_number(       root_object, "device.uptime",                                current_time - startup_time);
	        json_object_dotset_string(       root_object, "device.platform",                              platform);
	        json_object_dotset_string(       root_object, "device.email",                                 email);
	        json_object_dotset_string(       root_object, "device.description",                           description);
	        json_object_dotset_number(       root_object, "current.up_radio_packets_received",            cp_nb_rx_rcv);
	        json_object_dotset_number(       root_object, "current.up_radio_packets_crc_good",            cp_nb_rx_ok);
	        json_object_dotset_number(       root_object, "current.up_radio_packets_crc_bad",             cp_nb_rx_bad);
	        json_object_dotset_number(       root_object, "current.up_radio_packets_crc_absent",          cp_nb_rx_nocrc);
	        json_object_dotset_number(       root_object, "current.up_radio_packets_dropped",             cp_nb_rx_drop);
	        json_object_dotset_number(       root_object, "current.up_radio_packets_forwarded",           cp_up_pkt_fwd);
	        json_object_dotset_int_array(    root_object, "current.up_server_datagrams_send",             ar_up_dgram_sent,serv_count);
	        json_object_dotset_int_array(    root_object, "current.up_server_datagrams_acknowledged",     ar_up_ack_rcv,serv_count);
	        json_object_dotset_int_array(    root_object, "current.down_heartbeat_send",                  ar_dw_pull_sent,serv_count);
	        json_object_dotset_int_array(    root_object, "current.down_heartbeat_received",              ar_dw_ack_rcv,serv_count);
	        json_object_dotset_int_array(    root_object, "current.down_server_datagrams_received",       ar_dw_dgram_rcv,serv_count);
	        json_object_dotset_int_array(    root_object, "current.down_server_datagrams_accepted",       ar_dw_dgram_acp,serv_count);
	        json_object_dotset_number(       root_object, "current.down_radio_packets_succes",            cp_nb_tx_ok);
	        json_object_dotset_number(       root_object, "current.down_radio_packets_failure",           cp_nb_tx_fail);
	        json_object_dotset_number(       root_object, "current.down_radio_packets_collision_packet",  cp_nb_tx_rejected_collision_packet);
	        json_object_dotset_number(       root_object, "current.down_radio_packets_collision_beacon",  cp_nb_tx_rejected_collision_beacon);
	        json_object_dotset_number(       root_object, "current.down_radio_packets_too_early",         cp_nb_tx_rejected_too_early);
	        json_object_dotset_number(       root_object, "current.down_radio_packets_too_late",          cp_nb_tx_rejected_too_late);
	        json_object_dotset_number(       root_object, "current.down_beacon_packets_queued",           cp_nb_beacon_queued);
	        json_object_dotset_number(       root_object, "current.down_beacon_packets_send",             cp_nb_beacon_sent);
	        json_object_dotset_number(       root_object, "current.down_beacon_packets_rejected",         cp_nb_beacon_rejected);
	        json_object_dotset_number(       root_object, "performance.up_radio_packet_quality",          move_up_rx_quality);
	        json_object_dotset_double_array( root_object, "performance.up_server_datagram_quality",       move_up_ack_quality,serv_count);
	        json_object_dotset_double_array( root_object, "performance.down_server_heartbeat_quality",    move_dw_ack_quality,serv_count);
	        json_object_dotset_double_array( root_object, "performance.down_server_datagram_quality",     move_dw_datagram_quality,serv_count);
	        json_object_dotset_double_array( root_object, "performance.down_radio_packet_quality",        move_dw_receive_quality,serv_count);
	        json_object_dotset_number(       root_object, "performance.down_beacon_packet_quality",       move_dw_beacon_quality);
	        }
	        if (stat_file[0] != 0) {
	        	if (json_serialize_to_file_pretty(root_value,stat_file_tmp) == JSONSuccess)
	        		rename(stat_file_tmp,stat_file);
	        }
        pthread_mutex_lock(&mx_stat_rep);
        if (semtech_format == true) {
        	if ((gps_enabled == true) && (coord_ok == true)) {
				snprintf(status_report, STATUS_SIZE, "{\"time\":\"%s\",\"lati\":%.5f,\"long\":%.5f,\"alti\":%i,\"rxnb\":%u,\"rxok\":%u,\"rxfw\":%u,\"ackr\":%.1f,\"dwnb\":%u,\"txnb\":%u,\"pfrm\":\"%s\",\"mail\":\"%s\",\"desc\":\"%s\"}", stat_timestamp, cp_gps_coord.lat, cp_gps_coord.lon, cp_gps_coord.alt, cp_nb_rx_rcv, cp_nb_rx_ok, cp_up_pkt_fwd, 100.0 * up_ack_ratio, cp_dw_dgram_rcv, cp_nb_tx_ok,platform,email,description);
        	} else {
				snprintf(status_report, STATUS_SIZE, "{\"time\":\"%s\",\"rxnb\":%u,\"rxok\":%u,\"rxfw\":%u,\"ackr\":%.1f,\"dwnb\":%u,\"txnb\":%u,\"pfrm\":\"%s\",\"mail\":\"%s\",\"desc\":\"%s\"}", stat_timestamp, cp_nb_rx_rcv, cp_nb_rx_ok, cp_up_pkt_fwd, 100.0 * up_ack_ratio, cp_dw_dgram_rcv, cp_nb_tx_ok,platform,email,description);
        	}
	     	printf("# Semtech status report send. \n");
        } else if (lorank_format == true) {
            json_serialize_to_buffer(root_value,status_report,STATUS_SIZE);
            json_value_free(root_value);
	     	printf("# Lorank status report send. \n");
        } else 	{
        	printf("# NO status report send (format unknown!) \n");
        }

        report_ready = true;
        pthread_mutex_unlock(&mx_stat_rep);
		}
	    printf("##### END #####\n");
	}

    /* wait for upstream thread to finish (1 fetch cycle max) */
	if (upstream_enabled == true) pthread_join(thrid_up, NULL);
	if (downstream_enabled == true) {
		for (ic = 0; ic < serv_count; ic++)
			if (serv_live[ic] == true)
				pthread_join(thrid_down[ic], NULL);
    }
    //TODO: Dit heeft nawerk nodig
    pthread_cancel(thrid_jit); /* don't wait for jit thread */
    if (gps_active == true) pthread_cancel(thrid_timersync); /* don't wait for timer sync thread */

	if (ghoststream_enabled == true) ghost_stop();
	if (monitor_enabled == true) monitor_stop();
	if (gps_active == true) pthread_cancel(thrid_gps);   /* don't wait for GPS thread */
	if (gps_active == true) pthread_cancel(thrid_valid); /* don't wait for validation thread */

    /* if an exit signal was received, try to quit properly */
    if (exit_sig) {
        /* shut down network sockets */
		for (ic = 0; ic < serv_count; ic++) if (serv_live[ic] == true) {
			shutdown(sock_up[ic], SHUT_RDWR);
			shutdown(sock_down[ic], SHUT_RDWR);
		}
        /* stop the hardware */
		if (radiostream_enabled == true) {
        i = lgw_stop();
        if (i == LGW_HAL_SUCCESS) {
            MSG("INFO: concentrator stopped successfully\n");
        } else {
            MSG("WARNING: failed to stop concentrator successfully\n");
        }
    }
	}

    MSG("INFO: Exiting packet forwarder program\n");
    exit(EXIT_SUCCESS);
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 1: RECEIVING PACKETS AND FORWARDING THEM ---------------------- */

void thread_up(void) {
    int i, j; /* loop variables */
	int ic; /* Server Loop Variable */
    unsigned pkt_in_dgram; /* nb on Lora packet in the current datagram */

    /* allocate memory for packet fetching and processing */
    struct lgw_pkt_rx_s rxpkt[NB_PKT_MAX]; /* array containing inbound packets + metadata */
    struct lgw_pkt_rx_s *p; /* pointer on a RX packet */
    int nb_pkt;

	/* local timestamp variables until we get accurate GPS time */
	struct timespec fetch_time;
	struct tm * x1;
	char fetch_timestamp[28]; /* timestamp as a text string */

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

    /* report management variable */
    bool send_report = false;

	MSG("INFO: [up] Thread activated for all servers.\n");

    /* mote info variables */
    uint32_t mote_addr = 0;
    uint16_t mote_fcnt = 0;

    /* set upstream socket RX timeout */
	for (ic = 0; ic < serv_count; ic++) if (serv_live[ic] == true) {
		i = setsockopt(sock_up[ic], SOL_SOCKET, SO_RCVTIMEO, (void *)&push_timeout_half, sizeof push_timeout_half);
    if (i != 0) {
			MSG("ERROR: [up] setsockopt for server %s returned %s\n", serv_addr[ic], strerror(errno));
        exit(EXIT_FAILURE);
    }
	}

    /* pre-fill the data buffer with fixed fields */
    buff_up[0] = PROTOCOL_VERSION;
    buff_up[3] = PKT_PUSH_DATA;
    *(uint32_t *)(buff_up + 4) = net_mac_h;
    *(uint32_t *)(buff_up + 8) = net_mac_l;

    while (!exit_sig && !quit_sig) {

        /* fetch packets */
        pthread_mutex_lock(&mx_concent);
		if (radiostream_enabled == true) nb_pkt = lgw_receive(NB_PKT_MAX, rxpkt); else nb_pkt = 0;
		if (ghoststream_enabled == true) nb_pkt = ghost_get(NB_PKT_MAX-nb_pkt, &rxpkt[nb_pkt]) + nb_pkt;


        //TODO this test should in fact be before the ghost packets are collected.
        pthread_mutex_unlock(&mx_concent);
        if (nb_pkt == LGW_HAL_ERROR) {
            MSG("ERROR: [up] failed packet fetch, exiting\n");
            exit(EXIT_FAILURE);
        }

        /* check if there are status report to send */
        send_report = report_ready; /* copy the variable so it doesn't change mid-function */
        /* no mutex, we're only reading */

        /* wait a short time if no packets, nor status report */
        if ((nb_pkt == 0) && (send_report == false)) {
            wait_ms(FETCH_SLEEP_MS);
            continue;
        }

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

		/* local timestamp generation until we get accurate GPS time */
		clock_gettime(CLOCK_REALTIME, &fetch_time);
		x1 = gmtime(&(fetch_time.tv_sec)); /* split the UNIX timestamp to its calendar components */
		snprintf(fetch_timestamp, sizeof fetch_timestamp, "%04i-%02i-%02iT%02i:%02i:%02i.%06liZ", (x1->tm_year)+1900, (x1->tm_mon)+1, x1->tm_mday, x1->tm_hour, x1->tm_min, x1->tm_sec, (fetch_time.tv_nsec)/1000); /* ISO 8601 format */

        /* start composing datagram with the header */
        token_h = (uint8_t)rand(); /* random token */
        token_l = (uint8_t)rand(); /* random token */
        buff_up[1] = token_h;
        buff_up[2] = token_l;
        buff_index = 12; /* 12-byte header */

        /* start of JSON structure */
        memcpy((void *)(buff_up + buff_index), (void *)"{\"rxpk\":[", 9);
        buff_index += 9;

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
			//TODO: From the block below only one can be exectuted, decide on the presence of GPS.
			// This has not been coded well.
			if (gps_active) {
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
			} else {
				memcpy((void *)(buff_up + buff_index), (void *)",\"time\":\"???????????????????????????\"", 37);
				memcpy((void *)(buff_up + buff_index + 9), (void *)fetch_timestamp, 27);
				buff_index += 37;
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
                continue;
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
            report_ready = false;
            j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, "\"stat\":%s", status_report);
            pthread_mutex_unlock(&mx_stat_rep);
            if (j > 0) {
                buff_index += j;
            } else {
                MSG("ERROR: [up] failed to add field the status report to the transmission buffer.\n");
                continue; /* skip that packet*/
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
                meas_up_ack_rcv[ic] += 1;
                break;
            }
        }
        pthread_mutex_unlock(&mx_meas_up);
    }
	}
    MSG("\nINFO: End of upstream thread\n");
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 2: POLLING SERVER AND ENQUEUING PACKETS IN JIT QUEUE ---------- */

// TODO: factor this out and inspect the use of global variables. (Cause this is started for each server)

void thread_down(void* pic) {
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

    //TDOD: Check implications of multithreading
    /* JIT queue initialization */
    jit_queue_init(&jit_queue);

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

            /* Send acknoledge datagram to server */
            send_tx_ack(ic, buff_down[1], buff_down[2], jit_result);
        }
    }
    MSG("\nINFO: End of downstream thread\n");
}

void print_tx_status(uint8_t tx_status) {
    switch (tx_status) {
        case TX_OFF:
        	LOGGER("INFO: [jit] lgw_status returned TX_OFF\n");
            break;
        case TX_FREE:
        	LOGGER("INFO: [jit] lgw_status returned TX_FREE\n");
            break;
        case TX_EMITTING:
        	LOGGER("INFO: [jit] lgw_status returned TX_EMITTING\n");
            break;
        case TX_SCHEDULED:
        	LOGGER("INFO: [jit] lgw_status returned TX_SCHEDULED\n");
            break;
        default:
        	LOGGER("INFO: [jit] lgw_status returned UNKNOWN (%d)\n", tx_status);
            break;
    }
}


/* -------------------------------------------------------------------------- */
/* --- THREAD 3: CHECKING PACKETS TO BE SENT FROM JIT QUEUE AND SEND THEM --- */

void thread_jit(void) {
    int result = LGW_HAL_SUCCESS;
    struct lgw_pkt_tx_s pkt;
    int pkt_index = -1;
    struct timeval current_unix_time;
    struct timeval current_concentrator_time;
    enum jit_error_e jit_result;
    enum jit_pkt_type_e pkt_type;
    uint8_t tx_status;

	MSG("INFO: JIT thread activated.\n");

	while (!exit_sig && !quit_sig) {
        wait_ms(10);

        /* transfer data and metadata to the concentrator, and schedule TX */
        gettimeofday(&current_unix_time, NULL);
        get_concentrator_time(&current_concentrator_time, current_unix_time);
        jit_result = jit_peek(&jit_queue, &current_concentrator_time, &pkt_index);
        if (jit_result == JIT_ERROR_OK) {
            if (pkt_index > -1) {
                jit_result = jit_dequeue(&jit_queue, pkt_index, &pkt, &pkt_type);
                if (jit_result == JIT_ERROR_OK) {
                    /* update beacon stats */
                    if (pkt_type == JIT_PKT_TYPE_BEACON) {
                        pthread_mutex_lock(&mx_meas_dw);
                        meas_nb_beacon_sent += 1;
                        pthread_mutex_unlock(&mx_meas_dw);
                    }

                    /* check if concentrator is free for sending new packet */
                    result = lgw_status(TX_STATUS, &tx_status);
                    if (result == LGW_HAL_ERROR) {
                    	LOGGER("WARNING: [jit] lgw_status failed\n");
                    } else {
                        if (tx_status == TX_EMITTING) {
                        	LOGGER("ERROR: concentrator is currently emitting\n");
                            print_tx_status(tx_status);
                            continue;
                        } else if (tx_status == TX_SCHEDULED) {
                        	LOGGER("WARNING: a downlink was already scheduled, overwriting it...\n");
                            print_tx_status(tx_status);
                        } else {
                            /* Nothing to do */
                        }
                    }

                    /* send packet to concentrator */
                    pthread_mutex_lock(&mx_concent); /* may have to wait for a fetch to finish */
                    result = lgw_send(pkt);
                    pthread_mutex_unlock(&mx_concent); /* free concentrator ASAP */
                    if (result == LGW_HAL_ERROR) {
                        pthread_mutex_lock(&mx_meas_dw);
                        meas_nb_tx_fail += 1;
                        pthread_mutex_unlock(&mx_meas_dw);
                        LOGGER("WARNING: [jit] lgw_send failed\n");
                        continue;
                    } else {
                        pthread_mutex_lock(&mx_meas_dw);
                        meas_nb_tx_ok += 1;
                        pthread_mutex_unlock(&mx_meas_dw);
                        MSG_DEBUG(DEBUG_PKT_FWD, "lgw_send done: count_us=%u\n", pkt.count_us);
                    }
                } else {
                	LOGGER("ERROR: jit_dequeue failed with %d\n", jit_result);
                }
            }
        } else if (jit_result == JIT_ERROR_EMPTY) {
            /* Do nothing, it can happen */
        } else {
        	LOGGER("ERROR: jit_peek failed with %d\n", jit_result);
        }
    }

    MSG("\nINFO: End of JIT thread\n");
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 4: PARSE GPS MESSAGE AND KEEP GATEWAY IN SYNC ----------------- */

void thread_gps(void) {
    int i;

    /* serial variables */
    char serial_buff[128]; /* buffer to receive GPS data */
    ssize_t nb_char;

    /* variables for PPM pulse GPS synchronization */
    enum gps_msg latest_msg; /* keep track of latest NMEA message parsed */
    struct timespec utc_time; /* UTC time associated with PPS pulse */
    uint32_t trig_tstamp; /* concentrator timestamp associated with PPM pulse */

    /* position variable */
    struct coord_s coord;
    struct coord_s gpserr;

    /* initialize some variables before loop */
    memset(serial_buff, 0, sizeof serial_buff);

	MSG("INFO: GPS thread activated.\n");
	
    while (!exit_sig && !quit_sig) {
        /* blocking canonical read on serial port */
        nb_char = read(gps_tty_fd, serial_buff, sizeof(serial_buff)-1);
        if (nb_char <= 0) {
        	LOGGER("WARNING: [gps] read() returned value <= 0\n");
            continue;
        } else {
            serial_buff[nb_char] = 0; /* add null terminator, just to be sure */
        }

        /* parse the received NMEA */
        latest_msg = lgw_parse_nmea(serial_buff, sizeof(serial_buff));

        if (latest_msg == NMEA_RMC) { /* trigger sync only on RMC frames */

            /* get UTC time for synchronization */
            i = lgw_gps_get(&utc_time, NULL, NULL);
            if (i != LGW_GPS_SUCCESS) {
            	LOGGER("WARNING: [gps] could not get UTC time from GPS\n");
                continue;
            }

            /* get timestamp captured on PPM pulse  */
            pthread_mutex_lock(&mx_concent);
            i = lgw_get_trigcnt(&trig_tstamp);
            pthread_mutex_unlock(&mx_concent);
            if (i != LGW_HAL_SUCCESS) {
            	LOGGER("WARNING: [gps] failed to read concentrator timestamp\n");
                continue;
            }

            /* try to update time reference with the new UTC & timestamp */
            pthread_mutex_lock(&mx_timeref);
            i = lgw_gps_sync(&time_reference_gps, trig_tstamp, utc_time);
            pthread_mutex_unlock(&mx_timeref);
            if (i != LGW_GPS_SUCCESS) {
            	LOGGER("WARNING: [gps] GPS out of sync, keeping previous time reference\n");
                continue;
            }

            /* update gateway coordinates */
            i = lgw_gps_get(NULL, &coord, &gpserr);
            pthread_mutex_lock(&mx_meas_gps);
            if (i == LGW_GPS_SUCCESS) {
                gps_coord_valid = true;
                meas_gps_coord = coord;
                meas_gps_err = gpserr;
                // TODO: report other GPS statistics (typ. signal quality & integrity)
            } else {
                gps_coord_valid = false;
            }
            pthread_mutex_unlock(&mx_meas_gps);
        }
    }
    MSG("\nINFO: End of GPS thread\n");
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 5: CHECK TIME REFERENCE AND CALCULATE XTAL CORRECTION --------- */

void thread_valid(void) {

    /* GPS reference validation variables */
    long gps_ref_age = 0;
    bool ref_valid_local = false;
    double xtal_err_cpy;

    /* variables for XTAL correction averaging */
    unsigned init_cpt = 0;
    double init_acc = 0.0;
    double x;

	MSG("INFO: Validation thread activated.\n");
	
    /* correction debug */
    // FILE * log_file = NULL;
    // time_t now_time;
    // char log_name[64];

    /* initialization */
    // time(&now_time);
    // strftime(log_name,sizeof log_name,"xtal_err_%Y%m%dT%H%M%SZ.csv",localtime(&now_time));
    // log_file = fopen(log_name, "w");
    // setbuf(log_file, NULL);
    // fprintf(log_file,"\"xtal_correct\",\"XERR_INIT_AVG %u XERR_FILT_COEF %u\"\n", XERR_INIT_AVG, XERR_FILT_COEF); // DEBUG

    /* main loop task */
    while (!exit_sig && !quit_sig) {
        wait_ms(1000);

        /* calculate when the time reference was last updated */
        pthread_mutex_lock(&mx_timeref);
        gps_ref_age = (long)difftime(time(NULL), time_reference_gps.systime);
        if ((gps_ref_age >= 0) && (gps_ref_age <= GPS_REF_MAX_AGE)) {
            /* time ref is ok, validate and  */
            gps_ref_valid = true;
            ref_valid_local = true;
            xtal_err_cpy = time_reference_gps.xtal_err;
        } else {
            /* time ref is too old, invalidate */
            gps_ref_valid = false;
            ref_valid_local = false;
        }
        pthread_mutex_unlock(&mx_timeref);

        /* manage XTAL correction */
        if (ref_valid_local == false) {
            /* couldn't sync, or sync too old -> invalidate XTAL correction */
            pthread_mutex_lock(&mx_xcorr);
            xtal_correct_ok = false;
            xtal_correct = 1.0;
            pthread_mutex_unlock(&mx_xcorr);
            init_cpt = 0;
            init_acc = 0.0;
        } else {
            if (init_cpt < XERR_INIT_AVG) {
                /* initial accumulation */
                init_acc += xtal_err_cpy;
                ++init_cpt;
            } else if (init_cpt == XERR_INIT_AVG) {
                /* initial average calculation */
                pthread_mutex_lock(&mx_xcorr);
                xtal_correct = (double)(XERR_INIT_AVG) / init_acc;
                xtal_correct_ok = true;
                pthread_mutex_unlock(&mx_xcorr);
                ++init_cpt;
                // fprintf(log_file,"%.18lf,\"average\"\n", xtal_correct); // DEBUG
            } else {
                /* tracking with low-pass filter */
                x = 1 / xtal_err_cpy;
                pthread_mutex_lock(&mx_xcorr);
                xtal_correct = xtal_correct - xtal_correct/XERR_FILT_COEF + x/XERR_FILT_COEF;
                pthread_mutex_unlock(&mx_xcorr);
                // fprintf(log_file,"%.18lf,\"track\"\n", xtal_correct); // DEBUG
            }
        }
        MSG_DEBUG(DEBUG_LOG,"Time ref: %s, XTAL correct: %s (%.15lf)\n", ref_valid_local?"valid":"invalid", xtal_correct_ok?"valid":"invalid", xtal_correct); // DEBUG
    }
    MSG("\nINFO: End of validation thread\n");
}

/* --- EOF ------------------------------------------------------------------ */
