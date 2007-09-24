/* Calculate the size of physical memory.

   Copyright (C) 2000, 2003 Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.  */

/* Written by Paul Eggert; stolen from gnulib.  */
#ifndef PHYSMEM_H_
# define PHYSMEM_H_ 1

/* Returns the total amount of memory in the virtual memory system. */
double physmem_total (void);
/* Returns the total amount of available memory in the virtual memory
   system. */
double physmem_available (void);

#endif
