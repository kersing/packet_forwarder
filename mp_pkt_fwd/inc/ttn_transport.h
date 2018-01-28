/*
 * ttn_proto.h
 * 
 *  Created on: Dec 9, 2016
 *      Author: Jac Kersing
 */

#ifndef _TTN_PROTO_H
#define _TTN_PROTO_H
void ttn_init(int idx);
void ttn_upstream(void *pic);
void ttn_reconnect(int idx);
void ttn_connect(int idx);
void ttn_stop(int idx);
void ttn_data_up(int idx, int nb_pkt, struct lgw_pkt_rx_s *rxpkt);
void ttn_status_up(int idx, uint32_t rx_in, uint32_t rx_ok, uint32_t tx_in, uint32_t tx_ok);
void ttn_status(int idx);

#endif // _TTN_PROTO_H


