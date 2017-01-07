/*
 * semtech_proto.h
 * 
 *  Created on: Dec 5, 2016
 *      Author: Jac
 */

#ifndef _SEMTECH_PROTO_H
#define _SEMTECH_PROTO_H
void sem_init(void);
void sem_stop(void);
void sem_thread_down(void* pic);
void sem_data_up(int nb_pkt, struct lgw_pkt_rx_s *rxpkt, bool send_report);

#endif // _SEMTECH_PROTO_H


