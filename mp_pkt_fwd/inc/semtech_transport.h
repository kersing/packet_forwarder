/*
 * semtech_proto.h
 * 
 *  Created on: Dec 5, 2016
 *      Author: Jac
 */

#ifndef _SEMTECH_PROTO_H
#define _SEMTECH_PROTO_H
void semtech_init(int idx);
void semtech_stop(int idx);
void semtech_thread_down(void* pic);
void semtech_data_up(int idx, int nb_pkt, struct lgw_pkt_rx_s *rxpkt, bool send_report);
void semtech_upstream(void *pic);

#endif // _SEMTECH_PROTO_H


