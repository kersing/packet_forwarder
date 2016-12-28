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
#include "mp_pkt_fwd.h"
#include "ghost.h"
#include "monitor.h"
#include "semtech_proto.h"
#include "ttn_proto.h"

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

#define STATUS_SIZE		3072
#define TX_BUFF_SIZE    ((540 * NB_PKT_MAX) + 30 + STATUS_SIZE)

/* -------------------------------------------------------------------------- */
/* --- PRIVATE VARIABLES (GLOBAL) ------------------------------------------- */

/* signal handling variables */
volatile bool exit_sig = false; /* 1 -> application terminates cleanly (shut down hardware, close open files, etc) */
volatile bool quit_sig = false; /* 1 -> application terminates without shutting down the hardware */

/* packets filtering configuration variables */
bool fwd_valid_pkt = true; /* packets with PAYLOAD CRC OK are forwarded */
bool fwd_error_pkt = false; /* packets with PAYLOAD CRC ERROR are NOT forwarded */
bool fwd_nocrc_pkt = false; /* packets with NO PAYLOAD CRC are NOT forwarded */

/* network configuration variables */
uint8_t serv_count = 0; /* Counter for defined servers */
uint64_t lgwm = 0; /* Lora gateway MAC address */
char serv_addr[MAX_SERVERS][64]; /* addresses of the server (host name or IPv4/IPv6) */
char serv_port_up[MAX_SERVERS][8]; /* servers port for upstream traffic */
char serv_port_down[MAX_SERVERS][8]; /* servers port for downstream traffic */
bool serv_live[MAX_SERVERS];   /* Register if the server could be defined. */
time_t serv_contact[MAX_SERVERS]; /* last upstream contact time */
int serv_max_stall[MAX_SERVERS];  /* maximal acceptable down time of server. */
int keepalive_time = DEFAULT_KEEPALIVE; /* send a PULL_DATA request every X seconds, negative = disabled */

/* statistics collection configuration variables */
static unsigned stat_interval = DEFAULT_STAT; /* time interval (in sec) at which statistics are collected and displayed */

/* network protocol variables */
static struct timeval push_timeout_half = {0, (PUSH_TIMEOUT_MS * 500)}; /* cut in half, critical for throughput */

/* hardware access control and correction */
pthread_mutex_t mx_concent = PTHREAD_MUTEX_INITIALIZER; /* control access to the concentrator */
pthread_mutex_t mx_xcorr = PTHREAD_MUTEX_INITIALIZER; /* control access to the XTAL correction */
bool xtal_correct_ok = false; /* set true when XTAL correction is stable enough */
double xtal_correct = 1.0;

/* GPS configuration and synchronization */
static char gps_tty_path[64] = "\0"; /* path of the TTY port GPS is connected on */
static int gps_tty_fd = -1;          /* file descriptor of the GPS TTY port */
bool gps_active = false;      /* is GPS present and working on the board ? */

/* GPS time reference */
pthread_mutex_t mx_timeref = PTHREAD_MUTEX_INITIALIZER; /* control access to GPS time reference */
bool gps_ref_valid; /* is GPS reference acceptable (ie. not too old) */
struct tref time_reference_gps; /* time reference used for UTC <-> timestamp conversion */

/* Reference coordinates, for broadcasting (beacon) */
struct coord_s reference_coord;

/* Enable faking the GPS coordinates of the gateway */
//TODO: Now there are 4 different mutual dependent booleans to describe the gps state, this is a code smell, make an enumeration.
bool gps_fake_enable; /* fake coordinates override real coordinates */

/* measurements to establish statistics */
pthread_mutex_t mx_meas_up = PTHREAD_MUTEX_INITIALIZER; /* control access to the upstream measurements */
uint32_t meas_nb_rx_rcv = 0; /* count packets received */
uint32_t meas_nb_rx_ok = 0; /* count packets received with PAYLOAD CRC OK */
uint32_t meas_nb_rx_bad = 0; /* count packets received with PAYLOAD CRC ERROR */
uint32_t meas_nb_rx_nocrc = 0; /* count packets received with NO PAYLOAD CRC */
uint32_t meas_up_pkt_fwd = 0; /* number of radio packet forwarded to the server */
uint32_t meas_up_network_byte = 0; /* sum of UDP bytes sent for upstream traffic */
uint32_t meas_up_payload_byte = 0; /* sum of radio payload bytes sent for upstream traffic */
uint32_t meas_up_dgram_sent[MAX_SERVERS] = {0}; /* number of datagrams sent for upstream traffic */
uint32_t meas_up_ack_rcv[MAX_SERVERS] = {0}; /* number of datagrams acknowledged for upstream traffic */

pthread_mutex_t mx_meas_dw = PTHREAD_MUTEX_INITIALIZER; /* control access to the downstream measurements */
uint32_t meas_dw_pull_sent[MAX_SERVERS] = {0}; /* number of PULL requests sent for downstream traffic */
uint32_t meas_dw_ack_rcv[MAX_SERVERS] = {0}; /* number of PULL requests acknowledged for downstream traffic */
uint32_t meas_dw_dgram_rcv[MAX_SERVERS] = {0}; /* count PULL response datagrams received for downstream traffic */
uint32_t meas_dw_dgram_acp[MAX_SERVERS] = {0}; /* response datagrams that are accepted for transmission */
uint32_t meas_dw_network_byte = 0; /* sum of UDP bytes sent for upstream traffic */
uint32_t meas_dw_payload_byte = 0; /* sum of radio payload bytes sent for upstream traffic */
uint32_t meas_nb_tx_ok = 0; /* count packets emitted successfully */
uint32_t meas_nb_tx_fail = 0; /* count packets were TX failed for other reasons */
uint32_t meas_nb_tx_requested = 0; /* count TX request from server (downlinks) */
uint32_t meas_nb_tx_rejected_collision_packet = 0; /* count packets were TX request were rejected due to collision with another packet already programmed */
uint32_t meas_nb_tx_rejected_collision_beacon = 0; /* count packets were TX request were rejected due to collision with a beacon already programmed */
uint32_t meas_nb_tx_rejected_too_late = 0; /* count packets were TX request were rejected because it is too late to program it */
uint32_t meas_nb_tx_rejected_too_early = 0; /* count packets were TX request were rejected because timestamp is too much in advance */
uint32_t meas_nb_beacon_queued = 0; /* count beacon inserted in jit queue */
uint32_t meas_nb_beacon_sent = 0; /* count beacon actually sent to concentrator */
uint32_t meas_nb_beacon_rejected = 0; /* count beacon rejected for queuing */


pthread_mutex_t mx_meas_gps = PTHREAD_MUTEX_INITIALIZER; /* control access to the GPS statistics */
bool gps_coord_valid; /* could we get valid GPS coordinates ? */
struct coord_s meas_gps_coord; /* GPS position of the gateway */
struct coord_s meas_gps_err; /* GPS position of the gateway */

pthread_mutex_t mx_stat_rep = PTHREAD_MUTEX_INITIALIZER; /* control access to the status report */
static bool report_ready = false; /* true when there is a new report to send to the server */
char status_report[STATUS_SIZE]; /* status report as a JSON object */

/* beacon parameters */
uint32_t beacon_period = 0; /* set beaconing period, must be a sub-multiple of 86400, the nb of sec in a day */
uint32_t beacon_freq_hz = 0; /* TX beacon frequency, in Hz */

/* auto-quit function */
uint32_t autoquit_threshold = 0; /* enable auto-quit after a number of non-acknowledged PULL_DATA (0 = disabled)*/

//TODO: This default values are a code-smell, remove.
static char ghost_addr[64] = "127.0.0.1"; /* address of the server (host name or IPv4/IPv6) */
static char ghost_port[8]  = "1914";      /* port to listen on */

/* Variables to make the performance of forwarder locally available. */
static char stat_format[32] = "semtech";        /* format for json statistics. */
static char stat_file[1024] = "statistics.txt"; /* name / full path of file to store results in. */
static int stat_damping = 50;        /* default damping for statistical values. */

/* Just In Time TX scheduling */
struct jit_queue_s jit_queue;

//TODO: This default values are a code-smell, remove.
static char monitor_addr[64] = "127.0.0.1"; /* address of the server (host name or IPv4/IPv6) */
static char monitor_port[8]  = "2008";      /* port to listen on */

/* Gateway specificities */
int8_t antenna_gain = 0;

/* Control over the separate subprocesses. Per default, the system behaves like a basic packet forwarder. */
bool gps_enabled         = false;   /* controls the use of the GPS                      */
bool beacon_enabled      = false;   /* controls the activation of the time beacon.      */
static bool monitor_enabled     = false;   /* controls the activation access mode.             */
bool logger_enabled      = false;   /* controls the activation of more logging          */

/* TX capabilities */
struct lgw_tx_gain_lut_s txlut; /* TX gain table */
uint32_t tx_freq_min[LGW_RF_CHAIN_NB]; /* lowest frequency supported by TX chain */
uint32_t tx_freq_max[LGW_RF_CHAIN_NB]; /* highest frequency supported by TX chain */

/* Control over the separate streams. Per default, the system behaves like a basic packet forwarder. */
static bool upstream_enabled     = true;    /* controls the data flow from end-node to server         */
static bool downstream_enabled   = true;    /* controls the data flow from server to end-node         */
static bool ghoststream_enabled  = false;   /* controls the data flow from ghost-node to server       */
static bool radiostream_enabled  = true;    /* controls the data flow from radio-node to server       */
static bool statusstream_enabled = true;    /* controls the data flow of status information to server */

/* Informal status fields */
static char gateway_id[20]  = "";                /* string form of gateway mac address */
char platform[24]    = DISPLAY_PLATFORM;  /* platform definition */
char email[40]       = "";                /* used for contact email */
char description[64] = "";                /* used for free form description */
char ttn_gateway_id[64] = "";		  /* gateway ID for The Things Network */
char ttn_gateway_key[100] = "";		  /* gateway key to connect to TTN */

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

/* threads */
void thread_up(void);
//void thread_down(void* pic);
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
	JSON_Value *val3 = NULL; /* needed to detect the absence of some fields */
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
    	snprintf(gateway_id, sizeof gateway_id, "%s",str);
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
			val3 = json_object_get_value(nw_server, "serv_max_stall");
			/* Try to read the fields */
			if (str != NULL)  snprintf(serv_addr[ic], sizeof serv_addr[ic], "%s",str);
			if (val1 != NULL) snprintf(serv_port_up[ic], sizeof serv_port_up[ic], "%u", (uint16_t)json_value_get_number(val1));
			if (val2 != NULL) snprintf(serv_port_down[ic], sizeof serv_port_down[ic], "%u", (uint16_t)json_value_get_number(val2));
			if (val3 != NULL) serv_max_stall[ic] = (int) json_value_get_number(val3); else serv_max_stall[ic] = 0;
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
			snprintf(serv_addr[0], sizeof serv_addr[0], "%s",str);
			snprintf(serv_port_up[0], sizeof serv_port_up[0], "%u", (uint16_t)json_value_get_number(val1));
			snprintf(serv_port_down[0], sizeof serv_port_down[0], "%u", (uint16_t)json_value_get_number(val2));
			MSG("INFO: Server configured to \"%s\", with port up \"%s\" and port down \"%s\"\n", serv_addr[0],serv_port_up[0],serv_port_down[0]);
		}
	}


	/* Using the defaults in case no values are present in the JSON */
	//TODO: Eliminate this default behavior, the server should be well configured or stop.
	if (serv_count == 0) {
		MSG("INFO: Using defaults for server and ports (specific ports are ignored if no server is defined)");
		snprintf(serv_addr[0],sizeof(serv_addr[0]),STR(DEFAULT_SERVER));
		snprintf(serv_port_up[0],sizeof(serv_port_up[0]),STR(DEFAULT_PORT_UP));
		snprintf(serv_port_down[0],sizeof(serv_port_down[0]),STR(DEFAULT_PORT_DW));
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
			snprintf(mntr_sys_list[i], sizeof mntr_sys_list[i],"%s",str);
			MSG("INFO: System command %i: \"%s\"\n",i,mntr_sys_list[i]);
		}
	}

	/* monitor hostname or IP address (optional) */
	str = json_object_get_string(conf_obj, "monitor_address");
    if (str != NULL) {
    	snprintf(monitor_addr, sizeof monitor_addr,"%s",str);
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
		snprintf(ghost_addr, sizeof ghost_addr,"%s",str);
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
		snprintf(stat_format, sizeof stat_format,"%s",str);
		MSG("INFO: format is configured to \"%s\"\n", stat_format);
	}

	/* name of file to write statistical info to (optional) */
	str = json_object_get_string(conf_obj, "stat_file");
	if (str != NULL) {
		snprintf(stat_file, sizeof stat_file,"%s",str);
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
    	snprintf(gps_tty_path, sizeof gps_tty_path,"%s",str);
        MSG("INFO: GPS serial port path is configured to \"%s\"\n", gps_tty_path);
    }

	/* SSH path (optional) */
	str = json_object_get_string(conf_obj, "ssh_path");
	if (str != NULL) {
		snprintf(ssh_path, sizeof ssh_path,"%s",str);
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
		snprintf(ngrok_path, sizeof ngrok_path,"%s",str);
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
		if (strncmp(str, "*", 1) != 0) { snprintf(platform, sizeof platform,"%s",str); }
		MSG("INFO: Platform configured to \"%s\"\n", platform);
	}

	/* Read of contact email */
	str = json_object_get_string(conf_obj, "contact_email");
	if (str != NULL) {
		snprintf(email, sizeof email,"%s",str);
		MSG("INFO: Contact email configured to \"%s\"\n", email);
	}

	/* Read of description */
	str = json_object_get_string(conf_obj, "description");
	if (str != NULL) {
		snprintf(description, sizeof description,"%s",str);
		MSG("INFO: Description configured to \"%s\"\n", description);
	}

	/* Read of ttn_gateway_id */
	str = json_object_get_string(conf_obj, "ttn_gateway_id");
	if (str != NULL) {
		snprintf(ttn_gateway_id, sizeof ttn_gateway_id,"%s",str);
		MSG("INFO: TTN gateway ID configured to \"%s\"\n", ttn_gateway_id);
	}

	/* Read of ttn_gateway_key */
	str = json_object_get_string(conf_obj, "ttn_gateway_key");
	if (str != NULL) {
		snprintf(ttn_gateway_key, sizeof ttn_gateway_key,"%s",str);
		MSG("INFO: TTN gateway key configured to \"%s\"\n", ttn_gateway_key);
	}

    /* free JSON parsing data structure */
    json_value_free(root_val);
    return 0;
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
    int stall_time[MAX_SERVERS]       = {0};

    /* moving averages for overall statistics */
    double move_up_rx_quality                     =  0;   /* ratio of received crc_good packets over total received packets */
    double move_up_ack_quality[MAX_SERVERS]       = {0};  /* ratio of datagram sent to datagram acknowledged to server */
    double move_dw_ack_quality[MAX_SERVERS]       = {0};  /* ratio of pull request to pull response to server */
    double move_dw_datagram_quality[MAX_SERVERS]  = {0};  /* ratio of json correct datagrams to total datagrams received*/
    double move_dw_receive_quality[MAX_SERVERS]   = {0};  /* ratio of succesfully aired data packets to total received datapackets */
    double move_dw_beacon_quality                 =  0;   /* ratio of succesfully sent to queued for the beacon */

    /* GPS coordinates and variables */
    bool coord_ok = false;
    struct coord_s cp_gps_coord = {0.0, 0.0, 0};
    char gps_state[16] = "unknown";
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
    MSG("*** Multi Protocol Packet Forwarder for Lora Gateway ***\nVersion: " VERSION_STRING "\n");
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

    /* initialize semtech protocol stack */
    ttn_init();
    sem_init();

    /* sanity check on configuration variables */
    // TODO

	//TODO: Check if there are any live servers available, if not we should exit since there cannot be any
	// sensible course of action. Actually it would be best to redesign the whole communication loop, and take
	// the socket constructors to be inside a try-retry loop. That way we can respond to severs that implemented
	// there UDP handling erroneously, or any other temporal obstruction in the communication
	// path (broken stacks in routers for example) Now, contact may be lost for ever and a manual
	// restart at the this side is required.
	// => This has been 'resolved' bij allowing the forwarder to exit at stalled servers.

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
			i = pthread_create( &thrid_down[ic], NULL, (void * (*)(void *))sem_thread_down, (void *) (long) ic);
			if (i != 0) {
				MSG("ERROR: [main] impossible to create downstream thread\n");
				exit(EXIT_FAILURE);
			}
		}

    /* JIT queue initialization */
    jit_queue_init(&jit_queue);
    
    i = pthread_create( &thrid_jit, NULL, (void * (*)(void *))thread_jit, NULL);
    if (i != 0) {
        MSG("ERROR: [main] impossible to create JIT thread\n");
        exit(EXIT_FAILURE);
	}
    }

    // Timer synchronization needed for downstream ...
    if (gps_active == true || downstream_enabled == true) {
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
        /* get timestamp for statistics (must be done inside the lock) */
        current_time = time(NULL);
        for (i=0; i<serv_count; i++) { stall_time[i] = (int) (current_time - serv_contact[i]); }
        pthread_mutex_unlock(&mx_meas_up);

        /* Do the math */
        strftime(stat_timestamp, sizeof stat_timestamp, "%F %T %Z", gmtime(&current_time));
        strftime(iso_timestamp, sizeof stat_timestamp, "%FT%TZ", gmtime(&current_time));

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

        /* Determine the GPS state in human understandable form */
        { if      (gps_enabled == false)      snprintf(gps_state, sizeof gps_state, "disabled");
          else if (gps_fake_enable == true)   snprintf(gps_state, sizeof gps_state, "fake");
          else if (gps_active == false)       snprintf(gps_state, sizeof gps_state, "inactive");
          else if (gps_ref_valid == false)    snprintf(gps_state, sizeof gps_state, "searching");
          else                                snprintf(gps_state, sizeof gps_state, "locked"); }

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
     	bool semtech_format       =  strcmp(stat_format,"semtech") == 0;
     	bool lorank_idee_verbose  =  strcmp(stat_format,"idee_verbose")  == 0;
     	bool lorank_idee_concise  =  strcmp(stat_format,"idee_concise")  == 0;
     	bool has_stat_file        =  stat_file[0] != 0;
        JSON_Value *root_value_verbose    = NULL;
        JSON_Object *root_object_verbose  = NULL;
        JSON_Value *root_value_concise    = NULL;
        JSON_Object *root_object_concise  = NULL;
		if (statusstream_enabled == true || has_stat_file == true) {
	        root_value_verbose  = json_value_init_object();
	        root_object_verbose = json_value_get_object(root_value_verbose);
	    	JSON_Value *servers_array_value  = json_value_init_array();
	        JSON_Array *servers_array_object = json_value_get_array(servers_array_value);
	    	for (ic = 0; ic < serv_count; ic++)
	    	{ JSON_Value *sub_value = json_value_init_object();
	    	  JSON_Object *sub_object = json_value_get_object(sub_value);
	    	  json_object_set_string(sub_object, "name", serv_addr[ic]);
	    	  json_object_set_boolean(sub_object, "found", serv_live[ic] == true);
	    	  if (serv_live[ic] == true) json_object_set_number(sub_object, "last_seen", stall_time[ic]); else json_object_set_string(sub_object, "last_seen", "never");
	    	  json_array_append_value(servers_array_object,sub_value); }
	    	json_object_set_value(           root_object_verbose, "servers",                                      servers_array_value);
            json_object_set_string(          root_object_verbose, "time",                                         iso_timestamp);
            json_object_dotset_string(       root_object_verbose, "device.id",                                    gateway_id);
            json_object_dotset_boolean(      root_object_verbose, "device.up_active",                             upstream_enabled == true);
            json_object_dotset_boolean(      root_object_verbose, "device.down_active",                           downstream_enabled == true);
            json_object_dotset_number(       root_object_verbose, "device.latitude",                              cp_gps_coord.lat);
            json_object_dotset_number(       root_object_verbose, "device.longitude",                             cp_gps_coord.lon);
            json_object_dotset_number(       root_object_verbose, "device.altitude",                              cp_gps_coord.alt);
            json_object_dotset_number(       root_object_verbose, "device.uptime",                                current_time - startup_time);
            json_object_dotset_string(       root_object_verbose, "device.gps",                                   gps_state);
            json_object_dotset_string(       root_object_verbose, "device.platform",                              platform);
            json_object_dotset_string(       root_object_verbose, "device.email",                                 email);
            json_object_dotset_string(       root_object_verbose, "device.description",                           description);
            json_object_dotset_number(       root_object_verbose, "current.up_radio_packets_received",            cp_nb_rx_rcv);
            json_object_dotset_number(       root_object_verbose, "current.up_radio_packets_crc_good",            cp_nb_rx_ok);
            json_object_dotset_number(       root_object_verbose, "current.up_radio_packets_crc_bad",             cp_nb_rx_bad);
            json_object_dotset_number(       root_object_verbose, "current.up_radio_packets_crc_absent",          cp_nb_rx_nocrc);
            json_object_dotset_number(       root_object_verbose, "current.up_radio_packets_dropped",             cp_nb_rx_drop);
            json_object_dotset_number(       root_object_verbose, "current.up_radio_packets_forwarded",           cp_up_pkt_fwd);
            json_object_dotset_int_array(    root_object_verbose, "current.up_server_datagrams_send",             ar_up_dgram_sent,serv_count);
            json_object_dotset_int_array(    root_object_verbose, "current.up_server_datagrams_acknowledged",     ar_up_ack_rcv,serv_count);
            json_object_dotset_int_array(    root_object_verbose, "current.down_heartbeat_send",                  ar_dw_pull_sent,serv_count);
            json_object_dotset_int_array(    root_object_verbose, "current.down_heartbeat_received",              ar_dw_ack_rcv,serv_count);
            json_object_dotset_int_array(    root_object_verbose, "current.down_server_datagrams_received",       ar_dw_dgram_rcv,serv_count);
            json_object_dotset_int_array(    root_object_verbose, "current.down_server_datagrams_accepted",       ar_dw_dgram_acp,serv_count);
            json_object_dotset_number(       root_object_verbose, "current.down_radio_packets_succes",            cp_nb_tx_ok);
            json_object_dotset_number(       root_object_verbose, "current.down_radio_packets_failure",           cp_nb_tx_fail);
            json_object_dotset_number(       root_object_verbose, "current.down_radio_packets_collision_packet",  cp_nb_tx_rejected_collision_packet);
            json_object_dotset_number(       root_object_verbose, "current.down_radio_packets_collision_beacon",  cp_nb_tx_rejected_collision_beacon);
            json_object_dotset_number(       root_object_verbose, "current.down_radio_packets_too_early",         cp_nb_tx_rejected_too_early);
            json_object_dotset_number(       root_object_verbose, "current.down_radio_packets_too_late",          cp_nb_tx_rejected_too_late);
            json_object_dotset_number(       root_object_verbose, "current.down_beacon_packets_queued",           cp_nb_beacon_queued);
            json_object_dotset_number(       root_object_verbose, "current.down_beacon_packets_send",             cp_nb_beacon_sent);
            json_object_dotset_number(       root_object_verbose, "current.down_beacon_packets_rejected",         cp_nb_beacon_rejected);
            json_object_dotset_number(       root_object_verbose, "performance.up_radio_packet_quality",          move_up_rx_quality);
            json_object_dotset_double_array( root_object_verbose, "performance.up_server_datagram_quality",       move_up_ack_quality,serv_count);
            json_object_dotset_double_array( root_object_verbose, "performance.down_server_heartbeat_quality",    move_dw_ack_quality,serv_count);
            json_object_dotset_double_array( root_object_verbose, "performance.down_server_datagram_quality",     move_dw_datagram_quality,serv_count);
            json_object_dotset_double_array( root_object_verbose, "performance.down_radio_packet_quality",        move_dw_receive_quality,serv_count);
            json_object_dotset_number(       root_object_verbose, "performance.down_beacon_packet_quality",       move_dw_beacon_quality);
	        }
		if (statusstream_enabled == true && lorank_idee_concise) {
		    root_value_concise  = json_value_init_object();
	        root_object_concise = json_value_get_object(root_value_concise);
            json_object_dotset_string(       root_object_concise, "dev.id",         gateway_id);
            json_object_dotset_number(       root_object_concise, "dev.lat",        cp_gps_coord.lat);
            json_object_dotset_number(       root_object_concise, "dev.lon",        cp_gps_coord.lon);
            json_object_dotset_number(       root_object_concise, "dev.alt",        cp_gps_coord.alt);
            json_object_dotset_number(       root_object_concise, "dev.up",         current_time - startup_time);
            json_object_dotset_string(       root_object_concise, "dev.gps",        gps_state);
            json_object_dotset_string(       root_object_concise, "dev.pfrm",       platform);
            json_object_dotset_string(       root_object_concise, "dev.email",      email);
            json_object_dotset_string(       root_object_concise, "dev.desc",       description);
            if (upstream_enabled == true) {
              json_object_dotset_number(       root_object_concise, "prf.up_rf",      move_up_rx_quality);
              json_object_dotset_double_array( root_object_concise, "prf.up_srv_dg",  move_up_ack_quality,serv_count);
            }
            if (downstream_enabled == true) {
              json_object_dotset_double_array( root_object_concise, "prf.dw_srv_hb",  move_dw_ack_quality,serv_count);
              json_object_dotset_double_array( root_object_concise, "prf.dw_srv_dg",  move_dw_datagram_quality,serv_count);
              json_object_dotset_double_array( root_object_concise, "prf.dw_rf",      move_dw_receive_quality,serv_count);
              json_object_dotset_number(       root_object_concise, "prf.dw_bcn",     move_dw_beacon_quality);
            }
	    }
        if (has_stat_file == true) {
        	if (json_serialize_to_file_pretty(root_value_verbose,stat_file_tmp) == JSONSuccess)
        		rename(stat_file_tmp,stat_file);
        }
        if (statusstream_enabled == true) {
			pthread_mutex_lock(&mx_stat_rep);
			if (semtech_format == true) {
				if ((gps_enabled == true) && (coord_ok == true)) {
					snprintf(status_report, STATUS_SIZE, "{\"stat\":{\"time\":\"%s\",\"lati\":%.5f,\"long\":%.5f,\"alti\":%i,\"rxnb\":%u,\"rxok\":%u,\"rxfw\":%u,\"ackr\":%.1f,\"dwnb\":%u,\"txnb\":%u,\"pfrm\":\"%s\",\"mail\":\"%s\",\"desc\":\"%s\"}}", stat_timestamp, cp_gps_coord.lat, cp_gps_coord.lon, cp_gps_coord.alt, cp_nb_rx_rcv, cp_nb_rx_ok, cp_up_pkt_fwd, 100.0 * up_ack_ratio, cp_dw_dgram_rcv, cp_nb_tx_ok,platform,email,description);
				} else {
					snprintf(status_report, STATUS_SIZE, "{\"stat\":{\"time\":\"%s\",\"rxnb\":%u,\"rxok\":%u,\"rxfw\":%u,\"ackr\":%.1f,\"dwnb\":%u,\"txnb\":%u,\"pfrm\":\"%s\",\"mail\":\"%s\",\"desc\":\"%s\"}}", stat_timestamp, cp_nb_rx_rcv, cp_nb_rx_ok, cp_up_pkt_fwd, 100.0 * up_ack_ratio, cp_dw_dgram_rcv, cp_nb_tx_ok,platform,email,description);
				}
				printf("# Semtech status report send. \n");
			} else if (lorank_idee_verbose == true) {
				/* The time field is already permanently included in the packet stream, note that may be a little later. */
				json_object_remove(root_object_verbose,"time");
				json_serialize_to_buffer(root_value_verbose,status_report,STATUS_SIZE);
				printf("# Ideetron verbose status report send. \n");
			} else if (lorank_idee_concise == true) {
				json_serialize_to_buffer(root_value_concise,status_report,STATUS_SIZE);
				printf("# Ideetron concise status report send. \n");
			} else 	{
				printf("# NO status report send (format unknown!) \n");
			}
			report_ready = true;
			pthread_mutex_unlock(&mx_stat_rep);
		}
		if (statusstream_enabled == true || has_stat_file == true)  json_value_free(root_value_verbose);
		if (statusstream_enabled == true && lorank_idee_concise)    json_value_free(root_value_concise);
	    printf("##### END #####\n");

	    // Send status using TTN protocol
            ttn_status_up(cp_nb_rx_rcv, cp_nb_rx_ok, cp_nb_tx_ok + cp_nb_tx_fail, cp_nb_tx_ok);

	    /* Exit strategies. */
	    /* Server that are 'off-line may be a reason to exit */
	    for (ic = 0; ic < serv_count; ic++)
	    { if ( (serv_max_stall[ic] > 0) && (stall_time[ic] > serv_max_stall[ic]) )
	      { MSG("ERROR: [main] for server %s stalled for %i seconds, terminating packet forwarder.\n", serv_addr[ic], stall_time[ic]);
			exit(EXIT_FAILURE); } }

	    /* Code of gonzalocasas to catch transient hardware failures */
		uint32_t trig_cnt_us;
		if (lgw_get_trigcnt(&trig_cnt_us) == LGW_HAL_SUCCESS && trig_cnt_us == 0x7E000000) {
			MSG("ERROR: [main] unintended SX1301 reset detected, terminating packet forwarder.\n");
			exit(EXIT_FAILURE);
		}
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
        ttn_stop();
	sem_stop();
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
    //int ic; /* Server Loop Variable */

    /* allocate memory for packet fetching and processing */
    struct lgw_pkt_rx_s rxpkt[NB_PKT_MAX]; /* array containing inbound packets + metadata */
    int nb_pkt;

    /* report management variable */
    bool send_report = false;

    MSG("INFO: [up] Thread activated for all servers.\n");

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
        /* => ???, depends on memory model, caching, architecture and compiler optimization.
         *   However since the copying enforces a delayed decision, and the actual reading writing of
         *   the report is protected, it seems the worst that can happen is that you get an old report.
         */

        /* wait a short time if no packets, nor status report */
        if ((nb_pkt == 0) && (send_report == false)) {
            wait_ms(FETCH_SLEEP_MS);
            continue;
        }
		
        sem_data_up(nb_pkt, rxpkt, send_report);
        ttn_data_up(nb_pkt, rxpkt);
	if (send_report == true) {
		report_ready = false;
	}

    }
    MSG("\nINFO: End of upstream thread\n");
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 2: POLLING SERVER AND ENQUEUING PACKETS IN JIT QUEUE ---------- */
/* --- Moved to semtech_proto.c */
// TODO: factor this out and inspect the use of global variables. (Cause this is started for each server)

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
                        LOGGER("WARNING: [jit] lgw_send failed %d\n",result);
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
