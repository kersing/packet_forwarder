/*
 * Extension of Semtech Semtech-Cycleo Packet Forwarder.
 *  (C) 2015 Beta Research BV
 *
 *  Description: Monitor of the gateway.
 *
 *  License: Revised BSD License, see LICENSE.TXT file include in the project
 *  Maintainer: Ruud Vlaming
 */


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
#include "monitor.h"



/* -------------------------------------------------------------------------- */
/* --- PRIVATE MACROS ------------------------------------------------------- */

#define MSG(args...)    printf(args) /* message that is destined to the user */
#define PROTOCOL_VERSION    1
#define MNTR_DATA          17

volatile bool monitor_run = false;      /* false -> monitor thread terminates cleanly */

struct timeval monitor_timeout = {0, (200 * 1000)}; /* non critical for throughput */

static pthread_mutex_t cb_monitor = PTHREAD_MUTEX_INITIALIZER; /* control access to the monitor measurements */

//TODO: This default values are a code-smell, remove.
//static char monitor_addr[64] = "127.0.0.1"; /* address of the server (host name or IPv4/IPv6) */
//static char monitor_port[8]  = "2008";      /* port to listen on */

static char monitor_report[MONITOR_SIZE]; /* monitor report as a JSON object */

static int sock_monitor; /* socket for downstream traffic */

/* monitor thread */
static pthread_t thrid_monitor;


static void printBuffer(uint8_t *b, uint8_t len)
{ int i;
  for (i=0; i<len; i++) { printf("%i,",b[i]);  } }


static void thread_monitor(void);


/* -------------------------------------------------------------------------- */
/* --- THREAD: RECEIVING INFO FROM MONITOR SERVER --------------------------- */


void monitor_start(const char * monitor_addr, const char * monitor_port)
{
    /* You cannot start a running monitor listener.*/
    if (monitor_run) return;

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
    i = getaddrinfo(monitor_addr, monitor_port, &addresses, &result);
    if (i != 0)
    { MSG("ERROR: [up] getaddrinfo on address %s (PORT %s) returned %s\n", monitor_addr, monitor_port, gai_strerror(i));
      exit(EXIT_FAILURE); }

    /* try to open socket for monitor listener */
    for (q=result; q!=NULL; q=q->ai_next)
    { sock_monitor = socket(q->ai_family, q->ai_socktype,q->ai_protocol);
      if (sock_monitor == -1) continue; /* try next field */
      else break; }

    /* See if the connection was a success, if not, this is a permanent failure */
    if (q == NULL)
    { MSG("ERROR: [down] failed to open socket to any of server %s addresses (port %s)\n", monitor_addr, monitor_port);
      i = 1;
      for (q=result; q!=NULL; q=q->ai_next)
      { getnameinfo(q->ai_addr, q->ai_addrlen, host_name, sizeof host_name, port_name, sizeof port_name, NI_NUMERICHOST);
        MSG("INFO: [down] result %i host:%s service:%s\n", i, host_name, port_name);
        ++i; }
      exit(EXIT_FAILURE); }

    /* connect so we can send/receive packet with the server only */
    i = connect(sock_monitor, q->ai_addr, q->ai_addrlen);
    if (i != 0) {
        MSG("ERROR: [down] connect returned %s\n", strerror(errno));
        exit(EXIT_FAILURE); }

    freeaddrinfo(result);

    /* spawn thread to manage monitor connection */
    monitor_run = true;
    i = pthread_create( &thrid_monitor, NULL, (void * (*)(void *))thread_monitor, NULL);
    if (i != 0)
    { MSG("ERROR: [main] impossible to create monitor thread\n");
      exit(EXIT_FAILURE); }

    /* We are done here, monitor thread is initialized and should be running by now. */

}


void monitor_stop(void)
{   monitor_run = false;                /* terminate the loop. */
    pthread_cancel(thrid_monitor);      /* don't wait for downstream thread (is this okay??) */
    shutdown(sock_monitor, SHUT_RDWR);  /* close the socket. */
}


static void thread_monitor(void)
{   int i; /* loop variable */

    MSG("INFO: Monitor thread started.\n");

    /* local timekeeping variables */
    struct timespec send_time; /* time of the monitor request */
    struct timespec recv_time; /* time of return from recv socket call */

    /* data buffers */
    uint8_t buff_down[MNTR_RQST_MSGSIZE+1]; /* buffer to receive downstream packets */
    uint8_t buff_req[12]; /* buffer to compose pull requests */
    int msg_len;

    /* protocol variables */
    bool req_ack = false; /* keep track of whether PULL_DATA was acknowledged or not */

    /* set downstream socket RX timeout */
    i = setsockopt(sock_monitor, SOL_SOCKET, SO_RCVTIMEO, (void *)&monitor_timeout, sizeof monitor_timeout);
    if (i != 0)
    { MSG("ERROR: [down] setsockopt returned %s\n", strerror(errno));
      exit(EXIT_FAILURE); }

    /* pre-fill the pull request buffer with fixed fields */
    buff_req[0] = PROTOCOL_VERSION;
    buff_req[3] = MNTR_DATA;
    //TODO: repair random verification
    *(uint32_t *)(buff_req + 4) = 0;
    *(uint32_t *)(buff_req + 8) = 0;


    while (monitor_run)
    {   /* send PULL request and record time */
        // TODO zend later hier de data voor de nodes, nu alleen een pullreq.
        send(sock_monitor, (void *)buff_req, sizeof buff_req, 0);
        clock_gettime(CLOCK_MONOTONIC, &send_time);
        req_ack = false;
        MSG("MONITOR LOOP");
        /* listen to packets and process them until a new PULL request must be sent */
        recv_time = send_time;
        while ((int)difftimespec(recv_time, send_time) < MNTR_CALL_SECS)
        {   /* try to receive a datagram */
            msg_len = recv(sock_monitor, (void *)buff_down, (sizeof buff_down)-1, 0);
            clock_gettime(CLOCK_MONOTONIC, &recv_time);

            /* if a network message was received, reset the wait time, otherwise continue */
            if (msg_len >= 0)
            {  send_time = recv_time; }
            else
            { continue; }

            /* if the datagram does not respect protocol, just ignore it */
            if ( (msg_len != MNTR_RQST_MSGSIZE) || (buff_down[0] != PROTOCOL_VERSION) )
            { MSG("WARNING: [monitor] ignoring invalid request\n");
              continue; }

            /* the datagram is a MNTR_DATA */
            buff_down[msg_len] = 0; /* add string terminator, just to be safe */

            bool start_info = (buff_down[5] == 1);
            bool start_ssh = (buff_down[6] == 1);

            if (start_info) MSG("INFO: [monitor] please send info. \n");
            if (start_ssh) MSG("INFO: [monitor] please open ssh tunnel. \n");


           } }

    MSG("\nINFO: End of monitor thread\n");
}
