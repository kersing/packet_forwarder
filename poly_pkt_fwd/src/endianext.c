/*
 * endianext.c
 *
 *  Created on: Aug 5, 2016
 *      Author: ruud vlaming
 */


#include <stdint.h>     /* C99 types */
#include <stdbool.h>    /* bool type */
#include <string.h>     /* memset */
#include <stdlib.h>     /* atoi, exit */

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__

bool isBigEndian()    { return false; }
bool isMiddleEndian() { return false; }
bool isLittleEndian() { return true;  }

#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__

bool isBigEndian()    { return true;  }
bool isMiddleEndian() { return false; }
bool isLittleEndian() { return false; }

#else

bool isBigEndian()
{ uint8_t probe[4] = {0x01,0x23,0x45,0x67};
  uint32_t result;
  memcpy(&result,probe,4);
  return (result == 0x01234567);  }

bool isMiddleEndian()
{ uint8_t probe[4] = {0x01,0x23,0x45,0x67};
  uint32_t result;
  memcpy(&result,probe,4);
  return (result == 0x23016745);  }

bool isLittleEndian()
{ uint8_t probe[4] = {0x01,0x23,0x45,0x67};
  uint32_t result;
  memcpy(&result,probe,4);
  return (result == 0x67452301);  }

#endif

/* Like memcpy, but than with bytes reversed. As unsafe as memcpy too! */
void * swapcpy(void *dest, const void *src, size_t n)
{ size_t i;
  for(i=0;i<n;i++) ((char *)dest)[i] = ((char *)src)[n-i-1];
  return dest; }

/* Use this to copy int's double's etc. Ensures, by testing, the result
 * is in big Endian order.
 * Note: not as fast as byte swapping from byteswap.h, but some
 * systems do not have these available. And this works for all sizes. */
void * tobecpy(void *dest, const void *src, size_t n)
{ return (isLittleEndian() == true) ? swapcpy(dest,src,n) : memcpy(dest,src,n); }


