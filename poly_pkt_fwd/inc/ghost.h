/*
 *  Extension of Semtech Semtech-Cycleo Packet Forwarder.
 *  (C) 2015 Beta Research BV
 *
 *  Description: Virtualization of nodes.
 *
 *  License: Revised BSD License, see LICENSE.TXT file include in the project
 *  Maintainer: Ruud Vlaming
 */

#ifndef _GHOST_H_
#define _GHOST_H_

#include "loragw_hal.h"
#include "poly_pkt_fwd.h"

/* -------------------------------------------------------------------------- */
/* --- PRIVATE CONSTANTS AND FIELDS ------------------------------------------ */


/* Note that the ghost receive buffer must be large enough to store the
 * burst of data coming in just after the wait period in the main cycle
 * has commenced. In reality this wait state is one of the limiting factors
 * of the capacity. I am unaware of the number of packates the concentrator
 * can store, but my suspicion is 8. In this code the wait state is 10ms,
 * so this limits the maximum throughput in reality on: 8/0.01 = 800 packets/sec.
 * Although in burst mode it may be higher, since the wait time is not the
 * limiting factor at that moment.
 */

/* The total number of buffer bytes equals: (GHST_RX_BUFFSIZE + GHST_TX_BUFFSIZE) * GHST_NM_RCV */
#define GHST_MIN_PACKETSIZE   38     /* Minimal viable packet size. */
#define GHST_RX_BUFFSIZE     320     /* Size of buffer held for receiving packets  */
#define GHST_TX_BUFFSIZE     320     /* Size of buffer held for receiving packets  */
#define GHST_NM_RCV           12     /* max number of packets to be stored in receive mode, do not exceed 255  */
#define NODE_CALL_SECS        60     /* Minimum time between calls for ghost nodes, don't hammer de node server. */

/* -------------------------------------------------------------------------- */
/* --- PUBLIC FUNCTIONS PROTOTYPES ------------------------------------------ */



/* Call this to start/stop the server that communicates with the ghost node server. */
void ghost_start(const char * ghost_addr, const char * ghost_port);
void ghost_stop(void);


/* Call this to pull data from the receive buffer for ghost nodes.. */
int ghost_get(int max_pkt, struct lgw_pkt_rx_s *pkt_data);

/* Call this to push data from the server to the receiving ghost node.
 * Data is send immediately. */
int ghost_put();

#endif

/* --- EOF ------------------------------------------------------------------ */
