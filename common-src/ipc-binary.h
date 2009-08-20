/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Zmanda Inc., 465 N Mathlida Ave, Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#ifndef IPC_BINARY_H
#define IPC_BINARY_H

#include "amanda.h"

/* If you add a new command, it must replace CMD_MAX and CMD_MAX must be
 * increaed by one.
 */
#define CMD_DEVICE		0
#define REPLY_DEVICE		1
#define CMD_TAPE_OPEN		2
#define REPLY_TAPE_OPEN		3
#define CMD_TAPE_CLOSE		4
#define REPLY_TAPE_CLOSE	5
#define CMD_TAPE_MTIO   	6
#define REPLY_TAPE_MTIO		7
#define CMD_TAPE_WRITE		8
#define REPLY_TAPE_WRITE	9
#define CMD_TAPE_READ		10
#define REPLY_TAPE_READ		11
#define CMD_MAX			12

typedef struct amprotocol_s {
    uint16_t magic;
    int	     fd;
    int      number_of_args[CMD_MAX][2];
} amprotocol_t;

typedef struct command_s {
    uint16_t magic;
    uint16_t command;
    uint32_t block_size;
    uint32_t nb_arguments;
} command_t;

typedef struct argument_s {
    uint32_t  argument_size;
} argument_t;

typedef struct an_argument_s {
    uint32_t  size;
    char     *data;
} an_argument_t;

typedef struct amprotocol_packet_s {
    uint16_t       magic;
    uint16_t       command;
    uint32_t       block_size;
    uint32_t       nb_arguments;
    an_argument_t *arguments;
} amprotocol_packet_t;

ssize_t amprotocol_send(amprotocol_t *protocol, amprotocol_packet_t *packet);

amprotocol_packet_t *amprotocol_get(amprotocol_t *protocol);
amprotocol_packet_t * amprotocol_parse(amprotocol_t *protocol, char *buf_data, size_t len);

ssize_t amprotocol_send_list(amprotocol_t *protocol,
		int cmd, int nb_arguments, ...);
ssize_t amprotocol_send_binary(amprotocol_t *protocol,
		int cmd, int nb_arguments, ...);

void free_amprotocol_packet(amprotocol_packet_t *packet);

#endif /* IPC_BINARY_H */
