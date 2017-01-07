/*
 * ttn_proto.h
 * 
 *  Created on: Dec 9, 2016
 *      Author: Jac Kersing
 */

#ifndef _TTN_PROTO_H
#define _TTN_PROTO_H
int ttn_init(void);
void ttn_stop(void);
void ttn_data_up(int nb_pkt, struct lgw_pkt_rx_s *rxpkt);
void ttn_status_up(uint32_t rx_in, uint32_t rx_ok, uint32_t tx_in, uint32_t tx_ok);

#endif // _TTN_PROTO_H


