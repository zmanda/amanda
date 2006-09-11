
#
# Copyright (c) 1996,1997, by Sun Microsystems, Inc.
# All Rights Reserved
#
#ident	"@(#)Makefile	1.9	97/09/14 SMI"
#

TOP= ..

include $(TOP)/Makefile.master

.KEEP_STATE:

MOD=sst

OBJS=	sst.o

MOD_CONF= $(MOD:%=%.conf)

all: $(MOD)

include ../Makefile.driver

CFLAGS += -D$(INST) $(ENVCPPFLAGS1) $(ENVCPPFLAGS2) -DDEBUG

sstest: sstest.c
	$(CC) -o $(INST)/sstest $(ENVCPPFLAGS1) $(ENVCPPFLAGS2) sstest.c


DDICTFLAGS += -D_USCSI
