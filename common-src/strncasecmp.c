#include "amanda.h"

/*
 * The following function compares the first 'n' characters in 's1'
 * and 's2' without considering case. It returns less than, equal to
 * or greater than zero if 's1' is lexicographically less than, equal
 * to or greater than 's2'.
 */

int
strncasecmp(s1, s2, n)
      const char *s1;
      const char *s2;
      size_t n;
{
  unsigned char c1, c2;

  if ( (s1 == s2 ) || (n == 0) ) 
  {
      /*
       * the arguments are identical or there are no characters to be
       * compared
       */
      return 0;
  }

  while (n > 0)
  {
      c1 = (unsigned char)tolower(*s1++);
      c2 = (unsigned char)tolower(*s2++);

      if (c1 != c2)
      {
	  return(c1 - c2);
      }

      n--;
  }

  return(0);
}
