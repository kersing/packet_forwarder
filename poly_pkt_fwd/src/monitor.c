/*
 * Extension of Semtech Semtech-Cycleo Packet Forwarder.
 *  (C) 2015 Beta Research BV
 *
 *  Description: Monitor of the gateway.
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
#include "monitor.h"


/* -------------------------------------------------------------------------- */
/* --- SHARED FIELDS -------------------------------------------------------- */
uint16_t ssh_port         = 22;
uint16_t http_port        = 80;
char ssh_path[64]         = "/usr/bin/ssh";
char ngrok_path[64]       = "/usr/bin/ngrok";
int mntr_sys_count        = 0;
char mntr_sys_list[MNTR_SYS_MAX][64];


/* -------------------------------------------------------------------------- */
/* --- PRIVATE MACROS ------------------------------------------------------- */

#define MSG(args...)    printf(args) /* message that is destined to the user */
#define PROTOCOL_VERSION    1
#define MNTR_DATA          17
#define MNTR_RSPN          18

volatile bool monitor_run = false;      /* false -> monitor thread terminates cleanly */

struct timeval monitor_timeout = {0, (200 * 1000)}; /* non critical for throughput */

//!static pthread_mutex_t cb_monitor = PTHREAD_MUTEX_INITIALIZER; /* control access to the monitor measurements */

/* Format
 * Bytes 0,1,2,3:  Protocol definition
 * Byte  4:        Command type identifier
 * Bytes 5,6:      Action parameter
 * Bytes 7 .. 255  Optional arguments
 *
 * All instructions that cannot be interpreted maybe silently ignored, or an error message
 * may be sent.
 *
 * Byte 4: 0x00 => ignore message, this is an error
 *         0x01 => acknowledge of receipt, no action necessary
 *         0x02 => Execute an instruction from the shell instruction list.
 *         0x03 => open a ssh tunnel for ssh connection (usually on port 22)
 *         0x04 => close the ssh tunnel for ssh connection (usually on port 22)
 *         0x05 => open a ngrok tunnel for ssh connection (usually on port 22)
 *         0x06 => close the ngrok tunnel for ssh connection (usually on port 22)
 *         0x07 => open a ssh tunnel for web connection (usually on port 80)
 *         0x08 => close the ssh tunnel for web connection (usually on port 80)
 *         rest => for future use (ignore for now)
 *
 * In case of 0x02 the following byte contains the number of the instruction to be executed
 * Byte 5: 0x00 => other values are ignored
 * Byte 6: 0x00 => Send a list of all instructions
 *         0x01 => Execute first instruction etc
 *         rest => If the instruction does not exists send an "Error, missing instruction."
 *
 * In case of 0x03 / 0x05 the following two bytes contain the port number (as BE int value), and subsequently the port address (as string)
 * Byte 5: MSB of remote port
 * Byte 6: LSB of remote port
 * rest: null terminated string of address.
 *
 * Note: Total message should not exceed 255 bytes (rest is truncated)
 *       Total address should not exceed 192 bytes (rest is truncated)
 */



//!static char monitor_report[MONITOR_SIZE]; /* monitor report  */

static int sock_monitor; /* socket for downstream traffic */

static pid_t web_pid  = 0;
static pid_t ssh_pid  = 0;
static pid_t ngrok_pid  = 0;

/* monitor thread */
static pthread_t thrid_monitor;

static void thread_monitor(void);

static void system_call(const char * command, char * response, size_t respSize)
{ MSG("INFO: System call: %s\n",command);
  char buff[256];
  FILE *request;
  size_t recvcnt = 0;
  buff[255] = (char) 0;
  request = popen(command, "r");
  if (request == NULL)
  { snprintf(response,respSize,"Could not execute command: %s\n",command); }
  else
  { while(!feof(request) && (recvcnt + sizeof(buff) < respSize))
    { if (fgets(buff, sizeof(buff)-1, request) != NULL)
  	  { size_t len = strlen(buff);
	    memcpy(response+recvcnt,buff,len+1);
  	    recvcnt += len; } }
    response[recvcnt] = (char) 0; }
  pclose(request); }


// Note: you must have key installed in the local machine.
static pid_t system_ssh_tunnel_start(const char * address, const uint16_t remotePort, const uint16_t localPort)
{ char ssh[64] = "/usr/bin/ssh";  // dit moet in de config worden opgegeven
  char bridge[256];
  snprintf(bridge,sizeof(bridge),"%u:localhost:%u",remotePort,localPort);
  pid_t pid = fork();
  switch(pid)
  { case -1:
	  MSG("WARNING: Tunnel could not be setup.\n");
	  return 0;
    case 0:
      execl(ssh,"ssh","-n","-N","-R",bridge,address,NULL);
      MSG("WARNING: Tunnel died.\n");
      exit(0);
    default:
      MSG("INFO: Tunnel ssh[%i] started.\n",(int) pid);
      return pid; } }

static pid_t system_ngrok_tunnel_start(const uint16_t localPort)
{ char ngrok[64] = "/usr/bin/ngrok";  // dit moet in de config worden opgegeven
  char port[16];
  snprintf(port,sizeof(port),"%u",localPort);
  pid_t pid = fork();
  switch(pid)
  { case -1:
	  MSG("WARNING: Tunnel could not be setup.\n");
	  return 0;
    case 0:
      execl(ngrok,"ngrok","tcp",port,NULL);
      MSG("WARNING: Tunnel died.\n");
      exit(0);
    default:
      MSG("INFO: Tunnel ngrok[%i] started.\n", (int) pid);
      return pid; } }

static int system_tunnel_stop(pid_t pid)
{ if (pid > 0)
  { if (kill(pid,SIGTERM)==0)
    { MSG("INFO: Tunnel [%i] terminated.\n",(int) pid);
      return true; }
    else
    { MSG("WARNING: Tunnel [%i] could not be terminated.\n",(int) pid);
      return false; } }
  else
  { MSG("WARNING: No tunnel to terminate.\n");
    return true; } }


static void open_ssh(const char * address, const uint16_t remotePort)
{ MSG("INFO: Opening SSH tunnel for SSH on server %s at port %u\n",address,remotePort);
  if (ssh_pid > 0 || ngrok_pid > 0)
  { MSG("WARNING: Only one SSH tunnel can be active at a time.\n"); }
  else
  { ssh_pid = system_ssh_tunnel_start(address,remotePort,ssh_port); } }

static void close_ssh()
{ if (system_tunnel_stop(ssh_pid) == true) ssh_pid = 0; }

static void open_ngrok()
{ MSG("INFO: Opening ngrok tunnel for SSH.\n");
  if (ssh_pid > 0 || ngrok_pid > 0)
  { MSG("WARNING: Only one SSH tunnel can be active at a time.\n"); }
  else
  { ngrok_pid = system_ngrok_tunnel_start(ssh_port); } }

static void close_ngrok()
{ if (system_tunnel_stop(ngrok_pid) == true) ngrok_pid = 0; }

static void open_web(const char * address, const uint16_t remotePort)
{ MSG("INFO: Opening tunnel for WEB on server %s at port %u\n",address,remotePort);
  if (web_pid > 0)
  { MSG("WARNING: Only one SSH tunnel can be active at a time on local port %u.\n",http_port); }
  else
  { web_pid = system_ssh_tunnel_start(address,remotePort,http_port); } }

static void close_web()
{ if (system_tunnel_stop(web_pid) == true) web_pid = 0; }


static void printCmdList(char instructions[MNTR_SYS_MAX][64], unsigned int instCnt, char * response, size_t respSize)
{ unsigned int i;
  size_t respcnt = 0;
  for (i=0; i<instCnt; i++)
  { size_t len = strlen(instructions[i]);
    snprintf(response+respcnt,respSize-respcnt,"%s\n",instructions[i]);
    respcnt += len+1; } }


static void handleCommand(const uint8_t cmd, const uint16_t action, const char * arguments, char  * response, size_t respSize)
{ //unsigned int size = sizeof(instructions) / sizeof(*instructions);
  //MSG("DEBUG: Size of instruction = %u\n",size);
  switch(cmd)
  { case 0x02:
	  if      (action == 0)            { printCmdList(mntr_sys_list,mntr_sys_count,response,respSize); }
	  else if (action<=mntr_sys_count) { system_call(mntr_sys_list[action-1],response,respSize); }
	  else                             { snprintf(response,respSize,"Could not execute command with invalid action.\n"); }
	  break;
    case 0x03: open_ssh(arguments,action); break;
    case 0x04: close_ssh(); break;
    case 0x05: open_ngrok(); break;
    case 0x06: close_ngrok(); break;
    case 0x07: open_web(arguments,action); break;
    case 0x08: close_web(); break;
    default: MSG("WARNING: Unrecognized command, ignoring.");
  } }



/* -------------------------------------------------------------------------- */
/* --- THREAD: RECEIVING INFO FROM MONITOR SERVER --------------------------- */


void monitor_start(const char * monitor_addr, const char * monitor_port)
{
    /* You cannot start a running monitor listener.*/
    if (monitor_run) return;

    /* We do want to hear our children die. */
    signal(SIGCHLD, SIG_IGN);

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
    //TODO: repair the logic on this variable
    //bool req_ack = false; /* keep track of whether PULL_DATA was acknowledged or not */

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

    char response[4*256]; // UDP messages are generally not allowed to be very large. to be safe, keep them under 512 bytes.
    response[0] = PROTOCOL_VERSION;
    response[3] = MNTR_RSPN;

    while (monitor_run)
    {   /* send PULL request and record time */
        // TODO zend later hier de data voor de nodes, nu alleen een pullreq.
        send(sock_monitor, (void *)buff_req, sizeof buff_req, 0);
        clock_gettime(CLOCK_MONOTONIC, &send_time);
        //req_ack = false;
        //MSG("DEBUG: MONITOR LOOP\n");
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
            if ( (msg_len < 7) || (msg_len >= MNTR_RQST_MSGSIZE) || (buff_down[0] != PROTOCOL_VERSION) || (buff_down[3] != MNTR_DATA) )
            { MSG("WARNING: [monitor] ignoring invalid request\n");
              continue; }

            /* the datagram is a MNTR_DATA */
            buff_down[msg_len] = 0; /* add string terminator, just to be safe */

            uint8_t command   = (uint8_t) buff_down[4];
            uint16_t action   = (uint16_t) buff_down[5] << 8 | (uint16_t) buff_down[6];
            char * arguments  = (char *) buff_down+7;

            if (command > 0)
            { response[4] = 0;
              handleCommand(command,action,arguments,&response[4],sizeof(response)-4);
              size_t len = strlen(&response[4]);
              response[1] = (char) 0; //(len >> 8) & 0xFF; // to be implemented
              response[2] = (char) 0; //len & 0xFF; // to be implemented
              if (len > 0) send(sock_monitor, (void *)response, len+4, 0); }

           } }

    MSG("\nINFO: End of monitor thread\n");
}
