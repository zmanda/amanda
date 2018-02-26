#
# $Id: makefile,v 1.2 2001/08/10 17:12:22 ant Exp $
# Copyright (c) 1997 by Matthew Jacob
#
#	This software is free software; you can redistribute it and/or
#	modify it under the terms of the GNU Library General Public
#	License as published by the Free Software Foundation; version 2.
#
#	This software is distributed in the hope that it will be useful,
#	but WITHOUT ANY WARRANTY; without even the implied warranty of
#	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#	Library General Public License for more details.
#
#	You should have received a copy of the GNU Library General Public
#	License along with this software; if not, write to the Free
#	Software Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
#	The author may be reached via electronic communications at
#
#		mjacob@feral.com
#
#	or, via United States Postal Address
#
#		Matthew Jacob
#		1831 Castro Street
#		San Francisco, CA, 94131

#CC		= cc -qextchk -qlanglvl=ansi -qproto -qmaxmem=3000
CC		= gcc
RM		= rm -f
DEBUG_FLAG	=
KCFLAGS		= $(DEBUG_FLAG) -O
CFLAGS		= $(DEBUG_FLAG) -O

LDF1	=	-bI:/lib/kernex.exp -bI:/usr/lib/lowsys.exp
#LDF2	=	-lsys -lcsys

BINS	=	gscdd defgsc cfggsc ucfggsc tstinq
DATA	=	gsc.cat


all:	${BINS} ${DATA}

gscdd:	gscdd.o
	${LD} -bl:$@.map -o $@ $@.o -e gsc_config ${LDF1} ${LDF2}

gscdd.o:	gscdd.c gscdds.h
	$(CC) $(KCFLAGS) -c gscdd.c -DKERNEL -D_KERNEL  -D_IBMR2

cfggsc:	cfggsc.c
	$(CC) $(CFLAGS) -o $@ -lodm -lcfg $@.c

ucfggsc:	ucfggsc.c
	$(CC) $(CFLAGS) -o $@ -lodm -lcfg $@.c

defgsc:	defgsc.c
	$(CC) $(CFLAGS) -o $@ -lodm -lcfg $@.c

tstinq:	tstinq.c
	$(CC) $(CFLAGS) -o $@ $@.c

gsc.cat:	makefile
	${RM} /tmp/catalog_tmp
	echo '$$set 1 Feral Software Generic SCSI Messages' > /tmp/catalog_tmp
	echo "1 Feral Software Generic SCSI Release 1.0" >> /tmp/catalog_tmp
	gencat $@ /tmp/catalog_tmp
	${RM} /tmp/catalog_tmp

clean:
	@${RM} ${BINS} ${DATA} *.map *.o

