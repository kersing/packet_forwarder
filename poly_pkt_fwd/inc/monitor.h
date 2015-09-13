/*
 * Extension of Semtech Semtech-Cycleo Packet Forwarder.
 *  (C) 2015 Beta Research BV
 *
 *  Description: Monitor of the gateway.
 *
 *  License: Revised BSD License, see LICENSE.TXT file include in the project
 *  Maintainer: Ruud Vlaming
 */

#ifndef _ACCESS_H_
#define _ACCESS_H_


/*
 * At the moment the only requests that can come from the maintenance server are
 * - send monitor info. (on byte 5;  0: do not send, 1: send, others: ignore)
 * - open ssh tunnel    (on byte 6;  0: do nothing,  1: open, 2: close, others: ignore)
 * The message should exactly be 6 bytes long.
 * This will be json encoded in the future.
 * */

#include "poly_pkt_fwd.h"

/* -------------------------------------------------------------------------- */
/* --- PRIVATE CONSTANTS AND FIELDS ------------------------------------------ */

#define MNTR_RQST_MSGSIZE     128     /* Monitor request max size. */
#define MNTR_CALL_SECS         60     /* Minimum time between calls for monitor nodes, don't hammer de node server. */
#define MONITOR_SIZE          256     /* Maximal size of the monitor JSON information packet. */

/* -------------------------------------------------------------------------- */
/* --- PUBLIC FUNCTIONS PROTOTYPES ------------------------------------------ */

/* Call this to start/stop the server that communicates with the monitor node server. */
void monitor_start(const char * monitor_addr, const char * monitor_port);
void monitor_stop(void);

#endif


/* --- EOF ------------------------------------------------------------------ */
