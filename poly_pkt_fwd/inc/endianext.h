/*
 *  Extension of Semtech Semtech-Cycleo Packet Forwarder.
 *  (C) 2015 Beta Research BV
 *
 *  Description: Virtualization of nodes.
 *
 *  License: Revised BSD License, see LICENSE.TXT file include in the project
 *  Author & Maintainer (for this file): Ruud Vlaming
 */

#ifndef ENDIANEXT_H_
#define ENDIANEXT_H_

#include <stdbool.h>    /* bool type */

/* Functions to test for the Endianness of the hardware. One of the functions
 * should return true, and middle Endian is old. In most case a running
 * architecture is or Little or Big Endian, but in theory all functions
 * could return false. See here for more information:
 *   http://www.yolinux.com/TUTORIALS/Endian-Byte-Order.html
 *   http://unixpapa.com/incnote/byteorder.html
 * and of course good old wiki. */

bool isBigEndian();
bool isMiddleEndian();
bool isLittleEndian();


/* Like memcpy, but than with bytes reversed. As unsafe as memcpy too! */
void * swapcpy(void *dest, const void *src, size_t n);

/* ensures, by testing, the result is in big endian order.
 * Note: not as fast as byte swapping from byteswap.h, but some
 * systems do not have these available. And this works for all sizes. */
void * tobecpy(void *dest, const void *src, size_t n);

#endif /* ENDIANEXT_H_ */
