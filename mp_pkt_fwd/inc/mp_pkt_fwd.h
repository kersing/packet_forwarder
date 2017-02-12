/*
 * mp_pkt_fwd.h
 *
 *  Created on: Aug 28, 2015
 *      Author: ruud
 */

#ifndef _MP_PKT_FWD_H_
#define _MP_PKT_FWD_H_


/* -------------------------------------------------------------------------- */
/* --- MAC OSX Extensions  -------------------------------------------------- */

struct timespec;

#ifdef __MACH__
#define CLOCK_REALTIME 0
#define CLOCK_MONOTONIC 0
int clock_gettime(int clk_id, struct timespec* t);
#endif

double difftimespec(struct timespec end, struct timespec beginning);

#define MAX_SERVERS 4
#define NB_PKT_MAX      8 /* max number of packets per fetch/send cycle */

#define MIN_LORA_PREAMB 6 /* minimum Lora preamble length for this application */
#define STD_LORA_PREAMB 8
#define MIN_FSK_PREAMB  3 /* minimum FSK preamble length for this application */
#define STD_FSK_PREAMB  4


#endif /* _MP_PKT_FWD_H_ */
