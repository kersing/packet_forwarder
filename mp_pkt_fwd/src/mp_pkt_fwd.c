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
#include <getopt.h>
#include <limits.h>
#include <semaphore.h>

#include "trace.h"
#include "jitqueue.h"
#include "timersync.h"
#include "parson.h"
#include "base64.h"
#include "loragw_hal.h"
#include "loragw_gps.h"
#include "loragw_aux.h"
#include "loragw_reg.h"
#include "loragw_debug.h"
#include "config.h"
#include "mp_pkt_fwd.h"
#include "ghost.h"
#include "semtech_transport.h"
#include "connector.h"
#include "ttn_transport.h"
#include "transport.h"
#include "stats.h"

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

//TODO: This default values are a code-smell, remove.
#define DEFAULT_SERVER      127.0.0.1   /* hostname also supported */
#define DEFAULT_PORT_UP     1780
#define DEFAULT_PORT_DW     1782
#define DEFAULT_KEEPALIVE   5           /* default time interval for downstream keep-alive packet */
#define DEFAULT_STAT        30          /* default time interval for statistics */
#define PUSH_TIMEOUT_MS     100
#define PULL_TIMEOUT_MS     200
#define GPS_REF_MAX_AGE     30          /* maximum admitted delay in seconds of GPS loss before considering latest GPS sync unusable */
#define FETCH_SLEEP_MS      10          /* number of ms waited when a fetch return no packets */
#define BEACON_POLL_MS      50          /* time in ms between polling of beacon TX status */

#define PROTOCOL_VERSION    2           /* v1.3 */

#define XERR_INIT_AVG       128         /* number of measurements the XTAL correction is averaged on as initial value */
#define XERR_FILT_COEF      256         /* coefficient for low-pass XTAL error tracking */

#define PKT_PUSH_DATA   0
#define PKT_PUSH_ACK    1
#define PKT_PULL_DATA   2
#define PKT_PULL_RESP   3
#define PKT_PULL_ACK    4
#define PKT_TX_ACK      5

#define STATUS_SIZE     3072
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
int keepalive_time = DEFAULT_KEEPALIVE; /* send a PULL_DATA request every X seconds, negative = disabled */
Server servers[MAX_SERVERS];	/* Server information */

/* statistics collection configuration variables */
static unsigned stat_interval = DEFAULT_STAT; /* time interval (in sec) at which statistics are collected and displayed */

/* network protocol variables */
long push_timeout_ms = 100L;

/* hardware access control and correction */
pthread_mutex_t mx_concent = PTHREAD_MUTEX_INITIALIZER; /* control access to the concentrator */
pthread_mutex_t mx_xcorr = PTHREAD_MUTEX_INITIALIZER; /* control access to the XTAL correction */
bool xtal_correct_ok = false; /* set true when XTAL correction is stable enough */
double xtal_correct = 1.0;

/* GPS configuration and synchronization */
static char gps_tty_path[64] = "\0"; /* path of the TTY port GPS is connected on */
static int gps_tty_fd = -1;          /* file descriptor of the GPS TTY port */
bool gps_active = false;      /* is GPS present and working on the board? */

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


pthread_mutex_t mx_queues = PTHREAD_MUTEX_INITIALIZER; /* control access to the queues */
pthread_mutex_t mx_meas_gps = PTHREAD_MUTEX_INITIALIZER; /* control access to the GPS statistics */
bool gps_coord_valid; /* could we get valid GPS coordinates? */
struct coord_s meas_gps_coord; /* GPS position of the gateway */
struct coord_s meas_gps_err; /* GPS position of the gateway */

pthread_mutex_t mx_stat_rep = PTHREAD_MUTEX_INITIALIZER; /* control access to the status report */
bool report_ready = false; /* true when there is a new report to send to the server */
char status_report[STATUS_SIZE]; /* status report as a JSON object */

/* beacon parameters */
uint32_t beacon_period = 0; /* set beaconing period, must be a sub-multiple of 86400, the number of seconds in a day */
uint32_t beacon_freq_hz = 0; /* TX beacon frequency, in Hz */

/* auto-quit function */
uint32_t autoquit_threshold = 0; /* enable auto-quit after a number of non-acknowledged PULL_DATA (0 = disabled)*/

//TODO: This default values are a code-smell, remove.
static char ghost_addr[64] = "127.0.0.1"; /* address of the server (host name or IPv4/IPv6) */
static char ghost_port[8]  = "1914";      /* port to listen on */

/* Variables to make the performance of forwarder locally available. */
char stat_format[32] = "semtech";        /* format for json statistics. */
char stat_file[1024] = "\0";             /* name / full path of file to store results in, off by default. */
int stat_damping = 50;                   /* default damping for statistical values. */

/* Just In Time TX scheduling */
struct jit_queue_s jit_queue;

/* Gateway specifications */
int8_t antenna_gain = 0;

/* Control over the separate subprocesses. Per default, the system behaves like a basic packet forwarder. */
bool gps_enabled         = false;   /* controls the use of the GPS                      */
bool beacon_enabled      = false;   /* controls the activation of the time beacon.      */
bool logger_enabled      = false;   /* controls the activation of more logging          */
bool flush_enabled       = false;   /* flush output after statistics                    */
bool flush_line          = false;   /* flush output after every line                    */
bool wd_enabled          = false;   /* watchdog enabled                                 */

/* TX capabilities */
struct lgw_tx_gain_lut_s txlut; /* TX gain table */
uint32_t tx_freq_min[LGW_RF_CHAIN_NB]; /* lowest frequency supported by TX chain */
uint32_t tx_freq_max[LGW_RF_CHAIN_NB]; /* highest frequency supported by TX chain */

/* Control over the separate streams. Per default, the system behaves like a basic packet forwarder. */
bool upstream_enabled     = true;    /* controls the data flow from end-node to server         */
bool downstream_enabled   = true;    /* controls the data flow from server to end-node         */
bool statusstream_enabled = true;    /* controls the data flow of status information to server */
static bool ghoststream_enabled  = false;   /* controls the data flow from ghost-node to server       */
static bool radiostream_enabled  = true;    /* controls the data flow from radio-node to server       */

/* Informational status fields */
char gateway_id[20]  = "";                /* string form of gateway mac address */
char platform[24]    = DISPLAY_PLATFORM;  /* platform definition */
char email[40]       = "";                /* used for contact email */
char description[64] = "";                /* used for free form description */

/* Channel plan */
char *frequency_plan   = NULL;

/* timestamp for watchdog */
time_t last_loop;

/* path to logfile */
char *logfile_path = NULL;

/* enabled debugging options */
int debug_mask;

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

static void parse_cplan_configuration(const char * conf_file);

static int parse_SX1301_configuration(const char * conf_file);

static int parse_gateway_configuration(const char * conf_file);

/* threads */
void thread_up(void);
void thread_gps(void);
void thread_valid(void);
void thread_jit(void);
void thread_timersync(void);
void thread_watchdog(void);

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

static void parse_cplan_configuration(const char * conf_file) {
    const char conf_obj_name[] = "frequency_plan";
    const char *str;
    JSON_Value *root_val = NULL;
    JSON_Value *val = NULL;

    /* try to parse JSON */
    root_val = json_parse_file_with_comments(conf_file);
    if (root_val == NULL) {
        MSG("ERROR: %s is not a valid JSON file\n", conf_file);
        exit(EXIT_FAILURE);
    }

    val = json_object_get_value(json_value_get_object(root_val), conf_obj_name); /* fetch value (if possible) */
    if (val == NULL) {
        MSG("INFO: %s does not contain a JSON object named %s\n", conf_file, conf_obj_name);
        return;
    } else {
        MSG("INFO: %s does contain a JSON object named %s, parsing frequency_plan\n", conf_file, conf_obj_name);
    }

    if (json_value_get_type(val) == JSONString) {
        str = json_value_get_string(val); /* fetch value (if possible) */
        frequency_plan = strdup(str);
	if (frequency_plan == NULL) {
		MSG("ERROR: Unable to allocate memory for frequency_plan value");
	}
    } else {
        MSG("WARNING: Data type for frequency_plan seems wrong, please check\n");
        frequency_plan = NULL;
    }
    MSG("INFO: frequency plan: %s\n",frequency_plan != NULL ? frequency_plan : "not set" );
    json_value_free(root_val);
}

static int parse_SX1301_configuration(const char * conf_file) {
    int i;
    char param_name[32]; /* used to generate variable parameter names */
    const char *str; /* used to store string value from JSON object */
    const char conf_obj_name[] = "SX1301_conf";
    JSON_Value *root_val = NULL;
    JSON_Object *conf_obj = NULL;
    JSON_Object *conf_lbt_obj = NULL;
    JSON_Object *conf_lbtchan_obj = NULL;
    JSON_Value *val = NULL;
    JSON_Array *conf_array = NULL;
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
#ifndef _MULTITECH_H_
    if (json_value_get_type(val) == JSONNumber) {
        boardconf.clksrc = (uint8_t)json_value_get_number(val);
    } else {
        MSG("WARNING: Data type for clksrc seems wrong, please check\n");
        boardconf.clksrc = 0;
    }
#else
    // ignore parameter and hard code for MultiTech hardware
    boardconf.clksrc = 0;
#endif
    MSG("INFO: lorawan_public %d, clksrc %d\n", boardconf.lorawan_public, boardconf.clksrc);
    /* all parameters parsed, submitting configuration to the HAL */
    if (lgw_board_setconf(boardconf) != LGW_HAL_SUCCESS) {
        MSG("WARNING: Failed to configure board\n");
    }

    /* set LBT configuration */
    memset(&lbtconf, 0, sizeof lbtconf); /* initialize configuration structure */
    conf_lbt_obj = json_object_get_object(conf_obj, "lbt_cfg"); /* fetch value (if possible) */
    if (conf_lbt_obj == NULL) {
        MSG("INFO: no configuration for LBT\n");
    } else {
        val = json_object_get_value(conf_lbt_obj, "enable"); /* fetch value (if possible) */
        if (json_value_get_type(val) == JSONBoolean) {
            lbtconf.enable = (bool)json_value_get_boolean(val);
        } else {
            MSG("WARNING: Data type for lbt_cfg.enable seems wrong, please check\n");
            lbtconf.enable = false;
        }
        if (lbtconf.enable == true) {
            val = json_object_get_value(conf_lbt_obj, "rssi_target"); /* fetch value (if possible) */
            if (json_value_get_type(val) == JSONNumber) {
                lbtconf.rssi_target = (int8_t)json_value_get_number(val);
            } else {
                MSG("WARNING: Data type for lbt_cfg.rssi_target seems wrong, please check\n");
                lbtconf.rssi_target = 0;
            }
            val = json_object_get_value(conf_lbt_obj, "sx127x_rssi_offset"); /* fetch value (if possible) */
            if (json_value_get_type(val) == JSONNumber) {
                lbtconf.rssi_offset = (int8_t)json_value_get_number(val);
            } else {
                MSG("WARNING: Data type for lbt_cfg.sx127x_rssi_offset seems wrong, please check\n");
                lbtconf.rssi_offset = 0;
            }
            /* set LBT channels configuration */
            conf_array = json_object_get_array(conf_lbt_obj, "chan_cfg");
            if (conf_array != NULL) {
                lbtconf.nb_channel = json_array_get_count( conf_array );
                MSG("INFO: %u LBT channels configured\n", lbtconf.nb_channel);
            }
            for (i = 0; i < (int)lbtconf.nb_channel; i++) {
                /* Sanity check */
                if (i >= LBT_CHANNEL_FREQ_NB)
                {
                    MSG("ERROR: LBT channel %d not supported, skip it\n", i );
                    break;
                }
                /* Get LBT channel configuration object from array */
                conf_lbtchan_obj = json_array_get_object(conf_array, i);

                /* Channel frequency */
                val = json_object_dotget_value(conf_lbtchan_obj, "freq_hz"); /* fetch value (if possible) */
                if (json_value_get_type(val) == JSONNumber) {
                    lbtconf.channels[i].freq_hz = (uint32_t)json_value_get_number(val);
                } else {
                    MSG("WARNING: Data type for lbt_cfg.channels[%d].freq_hz seems wrong, please check\n", i);
                    lbtconf.channels[i].freq_hz = 0;
                }

                /* Channel scan time */
                val = json_object_dotget_value(conf_lbtchan_obj, "scan_time_us"); /* fetch value (if possible) */
                if (json_value_get_type(val) == JSONNumber) {
                    lbtconf.channels[i].scan_time_us = (uint16_t)json_value_get_number(val);
                } else {
                    MSG("WARNING: Data type for lbt_cfg.channels[%d].scan_time_us seems wrong, please check\n", i);
                    lbtconf.channels[i].scan_time_us = 0;
                }
            }

            /* all parameters parsed, submitting configuration to the HAL */
            if (lgw_lbt_setconf(lbtconf) != LGW_HAL_SUCCESS) {
                MSG("ERROR: Failed to configure LBT\n");
                return -1;
            }
        } else {
            MSG("INFO: LBT is disabled\n");
        }
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
	JSON_Value *val4 = NULL; /* needed to detect the absence of some fields */
	JSON_Value *val5 = NULL; /* needed to detect the absence of some fields */
	JSON_Array *confservers = NULL;
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
	confservers = json_object_get_array(conf_obj, "servers");
	if (confservers != NULL) {
		/* serv_count represents the maximal number of servers to be read. */
		serv_count = json_array_get_count(confservers);
		MSG("INFO: Found %i servers in array.\n", serv_count);
		ic = 0;
		for (i = 0; i < serv_count  && ic < MAX_SERVERS; i++) {
			const char *vtype = NULL, *vgwid = NULL, *vgwkey = NULL;
			JSON_Value *vcrit = NULL;
			nw_server = json_array_get_object(confservers,i);
			str = json_object_get_string(nw_server, "server_address");
			val = json_object_get_value(nw_server, "serv_enabled");
			val1 = json_object_get_value(nw_server, "serv_port_up");
			val2 = json_object_get_value(nw_server, "serv_port_down");
			val3 = json_object_get_value(nw_server, "serv_max_stall");
			val4 = json_object_get_value(nw_server, "serv_up_enabled");
			val5 = json_object_get_value(nw_server, "serv_down_enabled");
			vtype = json_object_get_string(nw_server, "serv_type");
			vgwid = json_object_get_string(nw_server, "serv_gw_id");
			vgwkey = json_object_get_string(nw_server, "serv_gw_key");
			vcrit = json_object_get_value(nw_server, "critical");

			/* Try to read the fields */
			if (str != NULL)  snprintf(servers[ic].addr, sizeof servers[ic].addr, "%s",str);
			if (val1 != NULL) snprintf(servers[ic].port_up, sizeof servers[ic].port_up, "%u", (uint16_t)json_value_get_number(val1));
			if (val2 != NULL) snprintf(servers[ic].port_down, sizeof servers[ic].port_down, "%u", (uint16_t)json_value_get_number(val2));
			if (val3 != NULL) servers[ic].max_stall = (int) json_value_get_number(val3); else servers[ic].max_stall = 0;
			if (val4 != NULL) servers[ic].upstream = (bool) json_value_get_boolean(val4); 
			if (val5 != NULL) servers[ic].downstream = (bool) json_value_get_boolean(val5); 
			if (vcrit != NULL) servers[ic].critical = (bool) json_value_get_boolean(vcrit); 
			/* If there is no server name we can only silently progress to the next entry */
			if (str == NULL) {
				continue;
			} else if (vtype != NULL) {
				if (!strncmp(vtype,"semtech",7)) {
					servers[ic].type = semtech;
				}
				else if (!strncmp(vtype,"ttn",3)) {
					servers[ic].type = ttn_gw_bridge;
				}
				else if (!strncmp(vtype,"gwtraf",6)) {
					servers[ic].type = gwtraf;
				}
				else {
					MSG("INFO: Skipping server \"%s\" with invalid server type\n", servers[ic].addr);
					continue;
				}
			} else {
				servers[ic].type = semtech;
			}
			/* For semtech protocol, if there are no ports report and progress to the next entry */
			if (servers[ic].type == semtech && ((val1 == NULL) || (val2 == NULL))) {
				MSG("INFO: Skipping server \"%s\" with at least one invalid port number\n", servers[ic].addr);
				continue;
			}
			/* For TTN gateway bridge, if there is no gateway id or no gateway key report and progress to next entry */
			if (servers[ic].type == ttn_gw_bridge) {
				if (vgwid == NULL) {
					MSG("INFO: Skipping server \"%s\" due to missing gateway id\n", servers[ic].addr);
					continue;
				} else {
					strncpy(servers[ic].gw_id, vgwid, sizeof servers[ic].gw_id);
				}
				if (vgwkey == NULL) {
					MSG("INFO: Skipping server \"%s\" due to missing gateway key\n", servers[ic].addr);
					continue;
				} else {
					strncpy(servers[ic].gw_key, vgwkey, sizeof servers[ic].gw_key);
				}
				if (strchr(servers[ic].addr,':') != NULL) {
					// port specified
					char *colpos = strchr(servers[ic].addr,':');
					*colpos = 0;
					servers[ic].gw_port = atoi(colpos+1);
				} else {
					servers[ic].gw_port = 1883;
				}
			}
		        /* If the server was explicitly disabled, report and progress to the next entry */
			if ( (val != NULL) && ((json_value_get_type(val)) == JSONBoolean) && ((bool)json_value_get_boolean(val) == false )) {
				MSG("INFO: Skipping disabled server \"%s\"\n", servers[ic].addr);
				continue;
			}
			
			/* All test survived, this is a valid server, report and increase server counter. */
			MSG("INFO: Server %i configured to \"%s\"\n", ic, servers[ic].addr);
			/* The server may be valid, it is not yet live. */
			servers[ic].enabled = true;
			servers[ic].live = false;
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
			servers[0].live = false;
			strncpy(servers[0].addr, str, sizeof servers[0].addr);
			snprintf(servers[0].port_up, sizeof servers[0].port_up, "%u", (uint16_t)json_value_get_number(val1));
			snprintf(servers[0].port_down, sizeof servers[0].port_down, "%u", (uint16_t)json_value_get_number(val2));
			MSG("INFO: Server configured to \"%s\", with port up \"%s\" and port down \"%s\"\n", servers[0].addr, servers[0].port_up, servers[0].port_down);
		}
	}

	/* Check for ttn configuration */
	val = json_object_get_value(conf_obj, "ttn_enable");
	if (json_value_get_type(val) == JSONBoolean && ((bool)json_value_get_boolean(val) == true)) {
		const char *id = NULL, *key = NULL, *addr = NULL;
		/* Read value of ttn_gateway_id */
		id = json_object_get_string(conf_obj, "ttn_gateway_id");
		key = json_object_get_string(conf_obj, "ttn_gateway_key");
		addr = json_object_get_string(conf_obj, "ttn_address");
		if (id != NULL && key != NULL && addr != NULL) {
			strncpy(servers[serv_count].addr, addr, sizeof servers[serv_count].addr);
			strncpy(servers[serv_count].gw_id, id, sizeof servers[serv_count].gw_id);
			strncpy(servers[serv_count].gw_key, key, sizeof servers[serv_count].gw_key);
			servers[serv_count].enabled = true;
			servers[serv_count].live = false;
			servers[serv_count].type = ttn_gw_bridge;
			MSG("INFO: TTN address configured to \"%s\"\n", servers[serv_count].addr);
			serv_count++;
		}
	}

	/* Using the defaults in case no values are present in the JSON */
	//TODO: Eliminate this default behavior, the server should be well configured or stop.
	if (serv_count == 0) {
		MSG("INFO: Using defaults for server and ports (specific ports are ignored if no server is defined)");
		snprintf(servers[0].addr,sizeof(servers[0].addr),STR(DEFAULT_SERVER));
		snprintf(servers[0].port_up,sizeof(servers[0].port_up),STR(DEFAULT_PORT_UP));
		snprintf(servers[0].port_down,sizeof(servers[0].port_down),STR(DEFAULT_PORT_DW));
		servers[0].live = false;
		serv_count = 1;
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
	/* limit because ttn-gateway-connector will disconnect due to mqtt timeouts */
	if (stat_interval > 60) {
		stat_interval = 55;
	}
        MSG("INFO: statistics display interval is configured to %u seconds\n", stat_interval);
    }

    /* get time-out value (in ms) for upstream datagrams (optional) */
    val = json_object_get_value(conf_obj, "push_timeout_ms");
    if (val != NULL) {
        push_timeout_ms = (long int)json_value_get_number(val);
        //push_timeout_half.tv_usec = 500 * (long int)json_value_get_number(val);
        MSG("INFO: upstream PUSH_DATA time-out is configured to %u ms\n", push_timeout_ms);
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


	/* Read the value for flush enabled data */
	val = json_object_get_value(conf_obj, "flush");
	if (json_value_get_type(val) == JSONBoolean) {
		flush_enabled = (bool)json_value_get_boolean(val);
	}
	if (flush_enabled == true) {
		MSG("INFO: Flush output after statistics is enabled\n");
	} else {
		MSG("INFO: Flush output after statistics is disabled\n");
    }

	/* Read the value for flush_line data */
	val = json_object_get_value(conf_obj, "flush_line");
	if (json_value_get_type(val) == JSONBoolean) {
		flush_line = (bool)json_value_get_boolean(val);
	}
	if (flush_line == true) {
		MSG("INFO: Flush after each line of output is enabled\n");
	} else {
		MSG("INFO: Flush after each line of output is disabled\n");
    }

	/* Read the value for watchdog data */
	val = json_object_get_value(conf_obj, "watchdog");
	if (json_value_get_type(val) == JSONBoolean) {
		wd_enabled = (bool)json_value_get_boolean(val);
	}
	if (wd_enabled == true) {
		MSG("INFO: Watchdog is enabled\n");
	} else {
		MSG("INFO: Watchdog is disabled\n");
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_aux");
    if (json_value_get_type(val) == JSONBoolean) {
        debug_aux = (bool)json_value_get_boolean(val);
	MSG("INFO: debug_aux is %s\n", debug_aux ? "enabled":"disabled");
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_spi");
    if (json_value_get_type(val) == JSONBoolean) {
        debug_spi = (bool)json_value_get_boolean(val);
	MSG("INFO: debug_spi is %s\n", debug_spi ? "enabled":"disabled");
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_reg");
    if (json_value_get_type(val) == JSONBoolean) {
        debug_reg = (bool)json_value_get_boolean(val);
	MSG("INFO: debug_reg is %s\n", debug_reg ? "enabled":"disabled");
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_hal");
    if (json_value_get_type(val) == JSONBoolean) {
        debug_hal = (bool)json_value_get_boolean(val);
	MSG("INFO: debug_hal is %s\n", debug_hal ? "enabled":"disabled");
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_gps");
    if (json_value_get_type(val) == JSONBoolean) {
        debug_gps = (bool)json_value_get_boolean(val);
	MSG("INFO: debug_gps is %s\n", debug_gps ? "enabled":"disabled");
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_gpio");
    if (json_value_get_type(val) == JSONBoolean) {
        debug_gpio = (bool)json_value_get_boolean(val);
	MSG("INFO: debug_gpio is %s\n", debug_gpio ? "enabled":"disabled");
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_lbt");
    if (json_value_get_type(val) == JSONBoolean) {
        debug_lbt = (bool)json_value_get_boolean(val);
	MSG("INFO: debug_lbt is %s\n", debug_lbt ? "enabled":"disabled");
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_pkt_fwd");
    if (json_value_get_type(val) == JSONBoolean) {
        if ( (bool)json_value_get_boolean(val) ) {
		debug_mask |= DEBUG_PKT_FWD;
		MSG("INFO: debug_pkt_fwd is enabled\n");
	} else {
		debug_mask &= ~DEBUG_PKT_FWD;
		MSG("INFO: debug_pkt_fwd is disabled\n");
	}
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_jit");
    if (json_value_get_type(val) == JSONBoolean) {
        if ( (bool)json_value_get_boolean(val) ) {
		debug_mask |= DEBUG_JIT;
		MSG("INFO: debug_jit is enabled\n");
	} else {
		debug_mask &= ~DEBUG_JIT;
		MSG("INFO: debug_jit is disabled\n");
	}
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_jit_error");
    if (json_value_get_type(val) == JSONBoolean) {
        if ( (bool)json_value_get_boolean(val) ) {
		debug_mask |= DEBUG_JIT_ERROR;
		MSG("INFO: debug_jit_error is enabled\n");
	} else {
		debug_mask &= ~DEBUG_JIT_ERROR;
		MSG("INFO: debug_jit_error is disabled\n");
	}
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_timersync");
    if (json_value_get_type(val) == JSONBoolean) {
        if ( (bool)json_value_get_boolean(val) ) {
		debug_mask |= DEBUG_TIMERSYNC;
		MSG("INFO: debug_timersync is enabled\n");
	} else {
		debug_mask &= ~DEBUG_TIMERSYNC;
		MSG("INFO: debug_timersync is disabled\n");
	}
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_beacon");
    if (json_value_get_type(val) == JSONBoolean) {
        if ( (bool)json_value_get_boolean(val) ) {
		debug_mask |= DEBUG_BEACON;
		MSG("INFO: debug_beacon is enabled\n");
	} else {
		debug_mask &= ~DEBUG_BEACON;
		MSG("INFO: debug_beacon is disabled\n");
	}
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_log");
    if (json_value_get_type(val) == JSONBoolean) {
        if ( (bool)json_value_get_boolean(val) ) {
		debug_mask |= DEBUG_LOG;
	} else {
		debug_mask &= ~DEBUG_LOG;
	}
    }

    /* Read the value for debug_* */
    val = json_object_get_value(conf_obj, "debug_follow");
    if (json_value_get_type(val) == JSONBoolean) {
        if ( (bool)json_value_get_boolean(val) ) {
		debug_mask |= DEBUG_FOLLOW;
	} else {
		debug_mask &= ~DEBUG_FOLLOW;
	}
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

	/* Read value of contact email */
	str = json_object_get_string(conf_obj, "contact_email");
	if (str != NULL) {
		snprintf(email, sizeof email,"%s",str);
		MSG("INFO: Contact email configured to \"%s\"\n", email);
	}

	/* Read value of description */
	str = json_object_get_string(conf_obj, "description");
	if (str != NULL) {
		snprintf(description, sizeof description,"%s",str);
		MSG("INFO: Description configured to \"%s\"\n", description);
	}

    /* free JSON parsing data structure */
    json_value_free(root_val);
    return 0;
}

double difftimespec(struct timespec end, struct timespec beginning) {
    double x;

    x = 1E-9 * (double)(end.tv_nsec - beginning.tv_nsec);
    x += (double)(end.tv_sec - beginning.tv_sec);

    return x;
}

void usage(char *proc_name) {
	fprintf(stderr, "Usage: %s [-c config_dir] [-l logfile] [-s spi speed in hz] [-d device file]\n", proc_name);
	exit(1);
}

static char *short_options = "c:l:s:d:h";
static struct option long_options[] = {
        {"config-dir", 1, 0, 'c'},
        {"logfile", 1, 0, 'l'},
        {"speed", 1, 0, 's'},
        {"device", 1, 0, 'd'},
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0},
};

void sighup_handler() {
    int logfile_fd;
    int old_logfile_fd = -1;

    FILE *logfile = NULL;
    if (logfile_path) {
        logfile = fopen(logfile_path, "a");
	if (logfile) {
            dup2(STDOUT_FILENO, old_logfile_fd);
	    logfile_fd = fileno(logfile);
	    dup2(logfile_fd, STDOUT_FILENO);
	    dup2(logfile_fd, STDERR_FILENO);
	    close(old_logfile_fd);
	} else {
	    printf("Error opening log file %s\n", logfile_path);
	    exit(1);
	}
    }
}

/* -------------------------------------------------------------------------- */
/* --- MAIN FUNCTION -------------------------------------------------------- */

int main(int argc, char *argv[])
{
    struct sigaction sigact; /* SIGQUIT&SIGINT&SIGTERM signal handling */
    struct sigaction sighupact; /* SIGHUP signal handling */
    int i; /* loop variable and temporary variable for return value */
    int ic; /* Server loop variable */

    /* configuration file related */
    char *global_cfg_name= "global_conf.json"; /* contain global (typically network-wide) configuration */
    char *local_cfg_name = "local_conf.json"; /* contain node specific configuration, overwrite global parameters for parameters that are defined in both */
    char *debug_cfg_name = "debug_conf.json"; /* if present, all other configuration files are ignored */

    int opt_ind = 0;

    char cfg_dir[PATH_MAX] = {0};
    char global_cfg_path[PATH_MAX] = {0};
    char local_cfg_path[PATH_MAX] = {0};
    char debug_cfg_path[PATH_MAX] = {0};
    char *proc_name = argv[0];
    char spi_dev[PATH_MAX] = {0};
    char spi_speed_s[20] = {0};
    long spi_speed;

    /* threads */
    pthread_t thrid_up;
    pthread_t thrid_gps;
    pthread_t thrid_valid;
    pthread_t thrid_jit;
    pthread_t thrid_timersync;
    pthread_t thrid_watchdog;

    /* fields to store the moment of activation. */
    time_t current_time = time(NULL);
    int stall_time[MAX_SERVERS]       = {0};

    while((i = getopt_long(argc, argv, short_options, long_options, &opt_ind)) >= 0) {
        switch(i) {
	  case 0:
	       break;
	  case 'c':
	       strncpy(cfg_dir, optarg, sizeof(cfg_dir)-2);
	       strcat(cfg_dir, "/");
	       break;
	  case 'l':
	       logfile_path = strdup(optarg);
	       if (logfile_path == NULL) {
		   printf("Error: can't save logfile name\n");
		   exit(1);
               }
	       break;
          case 's':
               spi_speed = atol(optarg);
	       if (spi_speed == 0L) {
		   printf("Error: specified SPI speed is invalid\n");
		   exit(1);
               }
	       sprintf(spi_speed_s, "LORAGW_SPEED=%ld", spi_speed);
	       putenv(spi_speed_s);
	       break;
          case 'd':
		sprintf(spi_dev, "LORAGW_SPI=%s",optarg);
		putenv(spi_dev);
		break;
	  default:
	       usage(proc_name);
	       break;
	}
    }

    snprintf(global_cfg_path, sizeof(global_cfg_path),  "%s%s", cfg_dir, global_cfg_name);
    snprintf(local_cfg_path, sizeof(local_cfg_path),  "%s%s", cfg_dir, local_cfg_name);
    snprintf(debug_cfg_path, sizeof(debug_cfg_path),  "%s%s", cfg_dir, debug_cfg_name);

    /* redirect stdout, stderr to logfile if specified */
    int logfile_fd;
    FILE *logfile = NULL;
    if (logfile_path) {
        logfile = fopen(logfile_path, "a");
	if (logfile) {
	    logfile_fd = fileno(logfile);
	    dup2(logfile_fd, STDOUT_FILENO);
	    dup2(logfile_fd, STDERR_FILENO);
	} else {
	    printf("Error opening log file %s\n", logfile_path);
	    exit(1);
	}
    }

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

    /* initialize transport data */
    transport_init();

    /* load configuration files */
    if (access(debug_cfg_path, R_OK) == 0) { /* if there is a debug conf, parse only the debug conf */
        MSG("INFO: found debug configuration file %s, parsing it\n", debug_cfg_path);
        MSG("INFO: other configuration files will be ignored\n");
        parse_cplan_configuration(debug_cfg_path);
        parse_SX1301_configuration(debug_cfg_path);
        parse_gateway_configuration(debug_cfg_path);
    } else if (access(global_cfg_path, R_OK) == 0) { /* if there is a global conf, parse it and then try to parse local conf  */
        MSG("INFO: found global configuration file %s, parsing it\n", global_cfg_path);
        parse_cplan_configuration(global_cfg_path);
        parse_SX1301_configuration(global_cfg_path);
        parse_gateway_configuration(global_cfg_path);
        if (access(local_cfg_path, R_OK) == 0) {
            MSG("INFO: found local configuration file %s, parsing it\n", local_cfg_path);
            MSG("INFO: redefined parameters will overwrite global parameters\n");
            parse_cplan_configuration(local_cfg_path);
            parse_SX1301_configuration(local_cfg_path);
            parse_gateway_configuration(local_cfg_path);
        }
    } else if (access(local_cfg_path, R_OK) == 0) { /* if there is only a local conf, parse it and that's all */
        MSG("INFO: found local configuration file %s, parsing it\n", local_cfg_path);
        parse_cplan_configuration(local_cfg_path);
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

    /* clear statistics */
    stats_init();

    /* initialize protocol stacks */
    transport_start();

    /* sanity check on configuration variables */
    // TODO

	//TODO: Check if there are any live servers available, if not we should exit since there cannot be any
	// sensible course of action. Actually it would be best to redesign the whole communication loop, and take
	// the socket constructors to be inside a try-retry loop. That way we can respond to severs that implemented
	// there UDP handling erroneously, or any other temporal obstruction in the communication
	// path (broken stacks in routers for example) Now, contact may be lost for ever and a manual
	// restart at this side is required.
	// => This has been 'resolved' by allowing the forwarder to exit at stalled servers.

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
		MSG("WARNING: Radio is disabled, radio packets cannot be sent or received.\n");
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
		for (ic = 0; ic < serv_count; ic++) if (servers[ic].live == true && servers[ic].type == semtech && servers[ic].downstream == true) {
			i = pthread_create( &servers[ic].t_down, NULL, (void * (*)(void *))semtech_thread_down, (void *) (long) ic);
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
    sigaction(SIGQUIT, &sigact, NULL); /* Ctrl-\ */

    sigemptyset(&sighupact.sa_mask);
    sighupact.sa_flags = 0;
    sighupact.sa_handler = sighup_handler;
    sigaction(SIGHUP, &sighupact, NULL); /* rotate logfile on HUP */
    signal(SIGPIPE, SIG_IGN);	/* ignore writes after closing socket */

	/* Start the ghost Listener */
    if (ghoststream_enabled == true) {
    	ghost_start(ghost_addr,ghost_port,reference_coord,gateway_id);
		MSG("INFO: [main] Ghost listener started, ghost packets can now be received.\n");
    }
	
    /* Check if we have anything to do */
    if ( (radiostream_enabled == false) && (ghoststream_enabled == false) && (statusstream_enabled == false) ) {
    	MSG("WARNING: [main] All streams have been disabled, gateway may be completely silent.\n");
    }

    /* spawn thread for watchdog */
    if (wd_enabled == true) {
	last_loop = time(NULL);
        i = pthread_create( &thrid_watchdog, NULL, (void * (*)(void *))thread_watchdog, NULL);
        if (i != 0) {
            MSG("ERROR: [main] impossible to create watchdog thread\n");
            exit(EXIT_FAILURE);
        }
    }

    /* main loop task : statistics transmission */
    while (!exit_sig && !quit_sig) {
        /* wait for next reporting interval */
        wait_ms(1000 * stat_interval);

	if (exit_sig || quit_sig) {
		break;
	}

	    // Create statistics report
	    stats_report();

	    /* Exit strategies. */
	    /* Server that are 'off-line may be a reason to exit */
	    /* move to semtech_transport in due time */
	    current_time = time(NULL);
	    pthread_mutex_lock(&mx_meas_up);
            for (i=0; i<serv_count; i++) { 
		if (servers[i].type == semtech) {
		    stall_time[i] = (int) (current_time - servers[i].contact); 
		}
	    }
            pthread_mutex_unlock(&mx_meas_up);
	    for (ic = 0; ic < serv_count; ic++) { 
	      if ( (servers[i].type == semtech) && (servers[ic].max_stall > 0) && (stall_time[ic] > servers[ic].max_stall) ) { 
		MSG("ERROR: [main] for server %s stalled for %i seconds, terminating packet forwarder.\n", servers[ic].addr, stall_time[ic]);
			exit(EXIT_FAILURE); 
	      } 
	    }

	    last_loop = time(NULL);

	    /* Code of gonzalocasas to catch transient hardware failures */
		uint32_t trig_cnt_us;
		pthread_mutex_lock(&mx_concent);
		if (lgw_get_trigcnt(&trig_cnt_us) == LGW_HAL_SUCCESS && trig_cnt_us == 0x7E000000) {
			MSG("ERROR: [main] unintended SX1301 reset detected, terminating packet forwarder.\n");
			exit(EXIT_FAILURE);
		}
		pthread_mutex_unlock(&mx_concent);
	}
    /* disable watchdog */
    if (wd_enabled == true) pthread_cancel(thrid_watchdog);

    /* wait for upstream thread to finish (1 fetch cycle max) */
	if (upstream_enabled == true) pthread_join(thrid_up, NULL);
    /* shut down transports */
    transport_stop();

    //TODO: Dit heeft nawerk nodig / This needs some more work
    pthread_cancel(thrid_jit); /* don't wait for jit thread */
    if (gps_active == true) pthread_cancel(thrid_timersync); /* don't wait for timer sync thread */

	if (ghoststream_enabled == true) ghost_stop();
	if (gps_active == true) pthread_cancel(thrid_gps);   /* don't wait for GPS thread */
	if (gps_active == true) pthread_cancel(thrid_valid); /* don't wait for validation thread */

    /* if an exit signal was received, try to quit properly */
    if (exit_sig) {
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
        pthread_mutex_unlock(&mx_concent);


        //TODO this test should in fact be before the ghost packets are collected.
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
		
	stats_data_up(nb_pkt, rxpkt);
        transport_data_up(nb_pkt, rxpkt, send_report);
	if (send_report == true) {
		report_ready = false;
	}

    }
    MSG("INFO: End of upstream thread\n");
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 2: POLLING SERVER AND ENQUEUING PACKETS IN JIT QUEUE ---------- */
/* --- Moved to semtech_transport.c */
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
			increment_down(BEACON_SENT);
                    }

                    /* check if concentrator is free for sending new packet */
		    pthread_mutex_lock(&mx_concent);
                    result = lgw_status(TX_STATUS, &tx_status);
		    pthread_mutex_unlock(&mx_concent);
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
			increment_down(TX_FAIL);
                        LOGGER("WARNING: [jit] lgw_send failed %d\n",result);
                        continue;
                    } else {
			increment_down(TX_OK);
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

    MSG("INFO: End of JIT thread\n");
}


/* -------------------------------------------------------------------------- */
/* --- THREAD 4: PARSE GPS MESSAGE AND KEEP GATEWAY IN SYNC ----------------- */

static void gps_process_sync(void) {
    struct timespec gps_time;
    struct timespec utc;
    uint32_t trig_tstamp; /* concentrator timestamp associated with PPM pulse */
    int i = lgw_gps_get(&utc, &gps_time, NULL, NULL);

    /* get GPS time for synchronization */
    if (i != LGW_GPS_SUCCESS) {
        MSG("WARNING: [gps] could not get GPS time from GPS\n");
        return;
    }

    /* get timestamp captured on PPM pulse  */
    pthread_mutex_lock(&mx_concent);
    i = lgw_get_trigcnt(&trig_tstamp);
    pthread_mutex_unlock(&mx_concent);
    if (i != LGW_HAL_SUCCESS) {
        MSG("WARNING: [gps] failed to read concentrator timestamp\n");
        return;
    }

    /* try to update time reference with the new GPS time & timestamp */
    pthread_mutex_lock(&mx_timeref);
    i = lgw_gps_sync(&time_reference_gps, trig_tstamp, utc, gps_time);
    pthread_mutex_unlock(&mx_timeref);
    if (i != LGW_GPS_SUCCESS) {
        MSG("WARNING: [gps] GPS out of sync, keeping previous time reference\n");
    }
}

static void gps_process_coords(void) {
    /* position variable */
    struct coord_s coord;
    struct coord_s gpserr;
    int    i = lgw_gps_get(NULL, NULL, &coord, &gpserr);

    /* update gateway coordinates */
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

void thread_gps(void) {
    /* serial variables */
    char serial_buff[128]; /* buffer to receive GPS data */
    size_t wr_idx = 0;     /* pointer to end of chars in buffer */

    /* variables for PPM pulse GPS synchronization */
    enum gps_msg latest_msg; /* keep track of latest NMEA message parsed */

    /* initialize some variables before loop */
    memset(serial_buff, 0, sizeof serial_buff);

    while (!exit_sig && !quit_sig) {
        size_t rd_idx = 0;
        size_t frame_end_idx = 0;

        /* blocking non-canonical read on serial port */
        ssize_t nb_char = read(gps_tty_fd, serial_buff + wr_idx, LGW_GPS_MIN_MSG_SIZE);
        if (nb_char <= 0) {
            MSG("WARNING: [gps] read() returned value %ld\n", nb_char);
            continue;
        }
        wr_idx += (size_t)nb_char;

        /*******************************************
         * Scan buffer for UBX/NMEA sync chars and *
         * attempt to decode frame if one is found *
         *******************************************/
        while(rd_idx < wr_idx) {
            size_t frame_size = 0;

            /* Scan buffer for UBX sync char */
            if(serial_buff[rd_idx] == (char)LGW_GPS_UBX_SYNC_CHAR) {

                /***********************
                 * Found UBX sync char *
                 ***********************/
                latest_msg = lgw_parse_ubx(&serial_buff[rd_idx], (wr_idx - rd_idx), &frame_size);

                if (frame_size > 0) {
                    if (latest_msg == INCOMPLETE) {
                        /* UBX header found but frame appears to be missing bytes */
                        frame_size = 0;
                    } else if (latest_msg == INVALID) {
                        /* message header received but message appears to be corrupted */
                        MSG("WARNING: [gps] could not get a valid message from GPS (no time)\n");
                        frame_size = 0;
                    } else if (latest_msg == UBX_NAV_TIMEGPS) {
                        gps_process_sync();
                    }
                }
            } else if(serial_buff[rd_idx] == LGW_GPS_NMEA_SYNC_CHAR) {
                /************************
                 * Found NMEA sync char *
                 ************************/
                /* scan for NMEA end marker (LF = 0x0a) */
                char* nmea_end_ptr = memchr(&serial_buff[rd_idx],(int)0x0a, (wr_idx - rd_idx));

                if(nmea_end_ptr) {
                    /* found end marker */
                    frame_size = nmea_end_ptr - &serial_buff[rd_idx] + 1;
                    latest_msg = lgw_parse_nmea(&serial_buff[rd_idx], frame_size);

                    if(latest_msg == INVALID || latest_msg == UNKNOWN) {
                        /* checksum failed */
                        frame_size = 0;
                    } else if (latest_msg == NMEA_GGA) { /* Get location from GGA frames */
                        gps_process_coords();
                    } else if (latest_msg == NMEA_RMC) { /* Get time/date from RMC frames */
                        gps_process_sync();
		    }
                }
            }

            if(frame_size > 0) {
                /* At this point message is a checksum verified frame
                   we're processed or ignored. Remove frame from buffer */
                rd_idx += frame_size;
                frame_end_idx = rd_idx;
            } else {
                rd_idx++;
            }
        } /* ...for(rd_idx = 0... */

        if(frame_end_idx) {
          /* Frames have been processed. Remove bytes to end of last processed frame */
          memcpy(serial_buff, &serial_buff[frame_end_idx], wr_idx - frame_end_idx);
          wr_idx -= frame_end_idx;
        } /* ...for(rd_idx = 0... */

        /* Prevent buffer overflow */
        if((sizeof(serial_buff) - wr_idx) < LGW_GPS_MIN_MSG_SIZE) {
            memcpy(serial_buff, &serial_buff[LGW_GPS_MIN_MSG_SIZE], wr_idx - LGW_GPS_MIN_MSG_SIZE);
            wr_idx -= LGW_GPS_MIN_MSG_SIZE;
        }
    }
    MSG("INFO: End of GPS thread\n");
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
    MSG("INFO: End of validation thread\n");
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 6: WATCHDOG TO CHECK IF THE SOFTWARE ACTUALLY FORWARDS DATA --- */

void thread_watchdog(void) {
    /* main loop task */
    while (!exit_sig && !quit_sig) {
        wait_ms(30000);
	// timestamp updated within the last 3 stat intervals? If not assume something is wrong and exit
	if ((time(NULL) - last_loop) > (long int)((stat_interval * 3) + 5)) {
		MSG("ERROR: Watchdog timer expired!\n");
		exit(254);
	}
    }
}

#include <stdarg.h>
/* -- Debugging aid */
void logmessage(const char *fmt, ... ) {
    time_t t;
    struct tm r;
    va_list argp;

    time(&t);
    localtime_r(&t, &r);
    printf("%02d:%02d:%02d  ",r.tm_hour,r.tm_min,r.tm_sec);

    va_start(argp, fmt);
    vprintf(fmt, argp);
    va_end(argp);
    fflush(stdout);
}


/* --- EOF ------------------------------------------------------------------ */
