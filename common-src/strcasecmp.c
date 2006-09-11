/* Provided by Michael Schmitz <mschmitz@sema.de>; public domain? */
#include "amanda.h"

/* Compare S1 and S2 ignoring case, returning less than,
   equal to or greater than zero if S1 is lexicographically
   less than, equal to or greater than S2.  */
int
strcasecmp(
    const char *s1,
    const char *s2)
{
  register const unsigned char *p1 = (const unsigned char *) s1;
  register const unsigned char *p2 = (const unsigned char *) s2;
  unsigned char c1, c2;

  if (p1 == p2)
    return 0;

  do {
      c1 = tolower (*p1++);
      c2 = tolower (*p2++);
      if (c1 == '\0' || c2 == '\0' || c1 != c2)
        return c1 - c2;
  } while ( 1 == 1 );
}
