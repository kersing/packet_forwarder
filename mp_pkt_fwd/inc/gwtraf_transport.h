/*
 * gwtraf_proto.h
 * 
 *  Created on: Jul 1, 2017
 *      Author: Jac
 */

#ifndef _GWTRAF_PROTO_H
#define _GWTRAF_PROTO_H
void gwtraf_init(int idx);
void gwtraf_stop(int idx);
void gwtraf_data_up(int idx, int nb_pkt, struct lgw_pkt_rx_s *rxpkt);
void gwtraf_upstream(void *pic);
void gwtraf_downtraf(int idx, char *json, int len);

#endif // _GWTRAF_PROTO_H


