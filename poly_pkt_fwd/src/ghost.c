/*
 *  Extension of Semtech Semtech-Cycleo Packet Forwarder.
 *  (C) 2015 Beta Research BV
 *
 *  Description: Virtualization of nodes.
 *
 *  License: Revised BSD License, see LICENSE.TXT file include in the project
 *  Maintainer: Ruud Vlaming
 */

/* fix an issue between POSIX and C99 */
#ifdef __MACH__
#elif __STDC_VERSION__ >= 199901L
	#define _XOPEN_SOURCE 600
#else
	#define _XOPEN_SOURCE 500
#endif

#include <stdint.h>     /* C99 types */
#include <stdbool.h>    /* bool type */
#include <stdio.h>      /* printf, fprintf, snprintf, fopen, fputs */

#include <string.h>     /* memset */
#include <signal.h>     /* sigaction */
#include <time.h>       /* time, clock_gettime, strftime, gmtime */
#include <sys/time.h>   /* timeval */
#include <unistd.h>     /* getopt, access */
#include <stdlib.h>     /* atoi, exit */
#include <errno.h>      /* error messages */

#include <sys/socket.h> /* socket specific definitions */
#include <netinet/in.h> /* INET constants and stuff */
#include <arpa/inet.h>  /* IP address conversion stuff */
#include <netdb.h>      /* gai_strerror */

#include <pthread.h>
#include "ghost.h"



/* -------------------------------------------------------------------------- */
/* --- PRIVATE MACROS ------------------------------------------------------- */

#define MSG(args...)    printf(args) /* message that is destined to the user */
#define PROTOCOL_VERSION    1
#define GHOST_DATA         11

volatile bool ghost_run = false;      /* false -> ghost thread terminates cleanly */

struct timeval ghost_timeout = {0, (200 * 1000)}; /* non critical for throughput */

static pthread_mutex_t cb_ghost = PTHREAD_MUTEX_INITIALIZER; /* control access to the ghoststream measurements */

//!static int rxpktSize = sizeof(struct lgw_pkt_rx_s);
//!static int txpktSize = sizeof(struct lgw_pkt_tx_s);

static uint8_t buffRX[GHST_RX_BUFFSIZE*GHST_NM_RCV]; /* circular buffer for receiving packets */
//!static uint8_t buffTX[GHST_TX_BUFFSIZE*GHST_NM_RCV]; /* circular buffer for sending packets */

static uint8_t ghst_end;                 /* end of circular packet buffer  */
static uint8_t ghst_bgn;                 /* begin of circular packet buffer */

static int sock_ghost; /* socket for downstream traffic */

/* ghost thread */
static pthread_t thrid_ghost;

/* for debugging purposes. */
static void printBuffer(uint8_t *b, uint8_t len)  __attribute__ ((unused));
static void printBuffer(uint8_t *b, uint8_t len)
{
  int i;
  for (i=0; i<len; i++) { printf("%i,",b[i]);  } }

/* for debugging purposes. */
static void printRX(struct lgw_pkt_rx_s *p)  __attribute__ ((unused));
static void printRX(struct lgw_pkt_rx_s *p)
{ printf(
    "  p->freq_hz    = %i\n"
    "  p->if_chain   = %i\n"
    "  p->status     = %i\n"
    "  p->count_us   = %i\n"
    "  p->rf_chain   = %i\n"
    "  p->modulation = %i\n"
    "  p->bandwidth  = %i\n"
    "  p->datarate   = %i\n"
    "  p->coderate   = %i\n"
    "  p->rssi       = %f\n"
    "  p->snr        = %f\n"
    "  p->snr_min    = %f\n"
    "  p->snr_max    = %f\n"
    "  p->crc        = %i\n"
    "  p->size       = %i\n"
    "  p->payload    = %s\n",
    p->freq_hz,p->if_chain,p->status,p->count_us,
    p->rf_chain,p->modulation,p->bandwidth,p->datarate,
    p->coderate,p->rssi,p->snr,p->snr_min,p->snr_max,
    p->crc,p->size,p->payload); }


/* to bitwise convert an unsigned integer to a float */
typedef union
{ uint32_t u;
  float f; } mix;

/* Helper functions for architecture independent conversion (BE!) of data packet to structure lgw_pkt_rx_s */
static uint32_t u32(uint8_t *p, uint8_t i) { return (uint32_t)(p[i+3]) + ((uint32_t)(p[i+2])<<8) + ((uint32_t)(p[i+1])<<16) + ((uint32_t)(p[i])<<24);  }
static uint16_t u16(uint8_t *p, uint8_t i) { return (uint16_t)p[i+1] + ((uint16_t)p[i]<<8);  }
static uint8_t u8(uint8_t *p, uint8_t i)   { return p[i]; }
static float eflt(uint8_t *p, uint8_t i)
{ mix uf;
  uf.u = u32(p,i);
  return uf.f; }

/* Method to fill lgw_pkt_rx_s with data received by the ghost node server. */
static void readRX(struct lgw_pkt_rx_s *p, uint8_t *b)
{ p->freq_hz    = u32(b,0);
  p->if_chain   =  u8(b,4);
  p->status     =  u8(b,5);
  p->count_us   = u32(b,6);
  p->rf_chain   =  u8(b,10);
  p->modulation =  u8(b,11);
  p->bandwidth  =  u8(b,12);
  p->datarate   = u32(b,13);
  p->coderate   =  u8(b,17);
  p->rssi       = eflt(b,18);
  p->snr        = eflt(b,22);
  p->snr_min    = eflt(b,26);
  p->snr_max    = eflt(b,30);
  p->crc        = u16(b,34);
  p->size       = u16(b,36);
  memcpy((p->payload),&b[38],p->size); }

static void thread_ghost(void);


/* -------------------------------------------------------------------------- */
/* --- THREAD: RECEIVING PACKETS FROM GHOST NODES --------------------------- */


void ghost_start(const char * ghost_addr, const char * ghost_port)
{
    /* You cannot start a running ghost listener.*/
    if (ghost_run) return;

    int i; /* loop variable and temporary variable for return value */

    struct addrinfo addresses;
    struct addrinfo *result; /* store result of getaddrinfo */
    struct addrinfo *q;      /* pointer to move into *result data */
    char host_name[64];
    char port_name[64];

    memset(&addresses, 0, sizeof addresses);
    addresses.ai_family = AF_UNSPEC;   /* should handle IP v4 or v6 automatically */
    addresses.ai_socktype = SOCK_DGRAM;

    /* Get the credentials for this server. */
    i = getaddrinfo(ghost_addr, ghost_port, &addresses, &result);
    if (i != 0)
    { MSG("ERROR: [up] getaddrinfo on address %s (PORT %s) returned %s\n", ghost_addr, ghost_port, gai_strerror(i));
      exit(EXIT_FAILURE); }

    /* try to open socket for ghost listener */
    for (q=result; q!=NULL; q=q->ai_next)
    { sock_ghost = socket(q->ai_family, q->ai_socktype,q->ai_protocol);
      if (sock_ghost == -1) continue; /* try next field */
      else break; }

    /* See if the connection was a success, if not, this is a permanent failure */
    if (q == NULL)
    { MSG("ERROR: [down] failed to open socket to any of server %s addresses (port %s)\n", ghost_addr, ghost_port);
      i = 1;
      for (q=result; q!=NULL; q=q->ai_next)
      { getnameinfo(q->ai_addr, q->ai_addrlen, host_name, sizeof host_name, port_name, sizeof port_name, NI_NUMERICHOST);
        MSG("INFO: [down] result %i host:%s service:%s\n", i, host_name, port_name);
        ++i; }
      exit(EXIT_FAILURE); }

    /* connect so we can send/receive packet with the server only */
    i = connect(sock_ghost, q->ai_addr, q->ai_addrlen);
    if (i != 0) {
        MSG("ERROR: [down] connect returned %s\n", strerror(errno));
        exit(EXIT_FAILURE); }

    freeaddrinfo(result);

    /* set the circular buffer pointers to the beginning */
    ghst_bgn = 0;
    ghst_end = 0;

    /* spawn thread to manage ghost connection */
    ghost_run = true;
    i = pthread_create( &thrid_ghost, NULL, (void * (*)(void *))thread_ghost, NULL);
    if (i != 0)
    { MSG("ERROR: [main] impossible to create ghost thread\n");
      exit(EXIT_FAILURE); }

    /* We are done here, ghost thread is initialized and should be running by now. */

}


void ghost_stop(void)
{   ghost_run = false;                /* terminate the loop. */
    pthread_cancel(thrid_ghost);      /* don't wait for downstream thread (is this okay??) */
    shutdown(sock_ghost, SHUT_RDWR);  /* close the socket. */
}



/* Call this to pull data from the receive buffer for ghost nodes.. */
int ghost_get(int max_pkt, struct lgw_pkt_rx_s *pkt_data)
{   /* Calculate the number of available packets */
    pthread_mutex_lock(&cb_ghost);
    uint8_t avail = (ghst_bgn - ghst_end + GHST_NM_RCV) % GHST_NM_RCV;
    pthread_mutex_unlock(&cb_ghost);

    /* Calculate the number of packets that we may or can copy */
    uint8_t get_pkt = (avail<max_pkt) ? avail : max_pkt;

    /* Do the actual copying, take into account that the read buffer is circular. */
    int i;
    for (i=0; i<get_pkt; i++)
    { int ind = (ghst_end + i) % GHST_NM_RCV;
      readRX(&pkt_data[i],(buffRX+ind*GHST_RX_BUFFSIZE)); }

    /* Shift the end index of the read buffer to the new position. */
    pthread_mutex_lock(&cb_ghost);
    ghst_end = (ghst_end + get_pkt) % GHST_NM_RCV;
    pthread_mutex_unlock(&cb_ghost);

    if (get_pkt>0)
    { MSG("INFO: copied %i packets from ghost, ghst_end  = %i \n",get_pkt,ghst_end);
      // To get more info enable this
      //for (i=0; i<get_pkt; i++)
      //{ printf("packet %i\n",i);
      //  printRX(&pkt_data[i]); }
    }

    /* return the number of packets that where copied. */
    return get_pkt; }


/* Call this to push data from the server to the receiving ghost node.
 * Data is send immediately. */
int ghost_put()
{   //TODO: Implement this method.
    return 0; }

static void thread_ghost(void)
{   int i; /* loop variable */

    MSG("INFO: Ghost thread started.\n");

    /* local timekeeping variables */
    struct timespec send_time; /* time of the pull request */
    struct timespec recv_time; /* time of return from recv socket call */

    /* data buffers */
    uint8_t buff_down[GHST_MIN_PACKETSIZE+GHST_RX_BUFFSIZE]; /* buffer to receive downstream packets */
    uint8_t buff_req[12]; /* buffer to compose pull requests */
    int msg_len;

    /* protocol variables */
    //TODO: repair the logic on this variable
    //!bool req_ack = false; /* keep track of whether PULL_DATA was acknowledged or not */

    /* set downstream socket RX timeout */
    i = setsockopt(sock_ghost, SOL_SOCKET, SO_RCVTIMEO, (void *)&ghost_timeout, sizeof ghost_timeout);
    if (i != 0)
    { MSG("ERROR: [down] setsockopt returned %s\n", strerror(errno));
      exit(EXIT_FAILURE); }

    /* pre-fill the pull request buffer with fixed fields */
    buff_req[0] = PROTOCOL_VERSION;
    buff_req[3] = GHOST_DATA;
    //TODO: repair random verification
    *(uint32_t *)(buff_req + 4) = 0;
    *(uint32_t *)(buff_req + 8) = 0;

    /* aux variable for data copy */
    int  next;
    bool full;

    while (ghost_run)
    {   /* send PULL request and record time */
        // TODO zend later hier de data voor de nodes, nu alleen een pullreq.
        send(sock_ghost, (void *)buff_req, sizeof buff_req, 0);
        clock_gettime(CLOCK_MONOTONIC, &send_time);
        //!req_ack = false;
        //MSG("DEBUG: GHOST LOOP\n");
        /* listen to packets and process them until a new PULL request must be sent */
        recv_time = send_time;
        while ((int)difftimespec(recv_time, send_time) < NODE_CALL_SECS)
        {   /* try to receive a datagram */
            msg_len = recv(sock_ghost, (void *)buff_down, (sizeof buff_down)-1, 0);
            clock_gettime(CLOCK_MONOTONIC, &recv_time);

            /* if a network message was received, reset the wait time, otherwise continue */
            if (msg_len >= 0)
            {  send_time = recv_time; }
            else
            { continue; }

            /* if the datagram does not respect protocol, just ignore it */
            if ((msg_len < 4 + GHST_MIN_PACKETSIZE) || (msg_len > 4 + GHST_RX_BUFFSIZE) || (buff_down[0] != PROTOCOL_VERSION) || ((buff_down[3] != GHOST_DATA) ))
            { MSG("WARNING: [down] ignoring invalid packet\n");
              continue; }

            /* the datagram is a GHOST_DATA */
            buff_down[msg_len] = 0; /* add string terminator, just to be safe */

            /* Determine the next pointer where data can be written and see if the circular buffer is full */
            next  =  (ghst_bgn + 1) % GHST_NM_RCV;
            pthread_mutex_lock(&cb_ghost);
            full  =  next == ghst_end;
            pthread_mutex_unlock(&cb_ghost);

            /* make a copy ot the data received to the circular buffer, and shift the write index. */
            if (full)
            {  MSG("WARNING: ghost buffer is full, dropping packet)\n"); }
            else
            {  memcpy((void *)(buffRX+ghst_bgn*GHST_RX_BUFFSIZE),buff_down+4,msg_len-3);
               pthread_mutex_lock(&cb_ghost);
               ghst_bgn = next;
               pthread_mutex_unlock(&cb_ghost);
               MSG("RECEIVED, ghst_bgn = %i \n", ghst_bgn); } } }

    MSG("\nINFO: End of ghost thread\n");
}
