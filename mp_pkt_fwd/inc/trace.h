/*
 / _____)             _              | |
( (____  _____ ____ _| |_ _____  ____| |__
 \____ \| ___ |    (_   _) ___ |/ ___)  _ \
 _____) ) ____| | | || |_| ____( (___| | | |
(______/|_____)_|_|_| \__)_____)\____)_| |_|
  (C)2013 Semtech-Cycleo

Description:
    LoRa concentrator : Packet Forwarder trace helpers

License: Revised BSD License, see LICENSE.TXT file include in the project
Maintainer: Michael Coracin
*/


#ifndef _LORA_PKTFWD_TRACE_H
#define _LORA_PKTFWD_TRACE_H

#define DEBUG_PKT_FWD   1
#define DEBUG_JIT       2
#define DEBUG_JIT_ERROR 4
#define DEBUG_TIMERSYNC 8
#define DEBUG_BEACON    16
#define DEBUG_LOG       32
#define DEBUG_FOLLOW    64

extern int debug_mask;

//#define MSG(args...) { printf(args) /* message that is destined to the user */; fflush(stdout); }

void logmessage(const char *, ...);
#define MSG(args...) logmessage(args)

#define MSG_DEBUG(FLAG, fmt, ...)                                                                         \
            do  {                                                                                         \
                if (FLAG & debug_mask)                                                                                 \
                    fprintf(stdout, "%s:%d:%s(): " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); \
            } while (0)

//#define LOGGER(args...) do { if (logger_enabled == true) printf(args); } while (0)
#define LOGGER(args...) do { if (logger_enabled == true) logmessage(args); } while (0)

#endif
/* --- EOF ------------------------------------------------------------------ */
