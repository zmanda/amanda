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

#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <errno.h>
#include <arpa/inet.h>
#include "ipc-binary.h"

static ssize_t amprotocol_read(int fd, void *buf, ssize_t count);
static ssize_t amprotocol_write(int fd, const void *buf, ssize_t count);
static uint32_t find_number_of_args(amprotocol_t *protocol, int cmd);

static ssize_t
amprotocol_read(
    int     fd,
    void   *buf,
    ssize_t  count)
{
    ssize_t  size = 0;
    ssize_t  rsize;
    char    *offset = (char *)buf;

    while (size < count) {
	rsize = read(fd, offset, count);
	if (rsize == 0) {
	    return size;
	} else if (rsize < 0) {
	    if (errno == EINTR)
		continue;
	    if (size > 0)
		return size;
	    return -1;
	}
	size += rsize;
	offset += rsize;
	count -= rsize;
    }
    return size;
}

static ssize_t
amprotocol_write(
    int fd,
    const void *buf,
    ssize_t count)
{
    ssize_t  size = 0;
    ssize_t  wsize;
    char    *offset = (char *)buf;

    while (size < count) {
	wsize = write(fd, offset, count);
	if (wsize == 0) {
	    return size;
	} else if (wsize < 0) {
	    if (errno == EINTR)
		continue;
	    if (size > 0)
		return size;
	    return -1;
	}
	size += wsize;
	offset += wsize;
	count -= wsize;
    }
    return size;
}

static uint32_t 
find_number_of_args(
    amprotocol_t *protocol,
    int           cmd)
{
    int      i;

    for (i=0; protocol->number_of_args[i][0] != CMD_MAX; i++) {
	if (protocol->number_of_args[i][0] == cmd)
	    return protocol->number_of_args[i][1];
    }

    return 0;
}

ssize_t
amprotocol_send(
    amprotocol_t        *protocol,
    amprotocol_packet_t *packet)
{
    command_t   *command;
    argument_t  *argument;
    char        *buffer, *offset;
    int          block_size;
    uint32_t     i;
    int          argument_size;

    if (packet->command >= CMD_MAX) {
	errno = EINVAL;
	return -1;
    }
    if (packet->nb_arguments != find_number_of_args(protocol, packet->command)) {
	errno = EINVAL;
	return -1;
    }
    block_size = sizeof(command_t);
    for (i=0; i<packet->nb_arguments; i++) {
	block_size += sizeof(argument_t) + packet->arguments[i].size;
    }

    buffer = offset = malloc(block_size);
    command = (command_t *)offset;
    command->magic = htons(protocol->magic);
    command->command = htons(packet->command);
    command->block_size = htonl(block_size);
    command->nb_arguments = htonl(packet->nb_arguments);
    offset += sizeof(command_t);
    for (i=0; i<packet->nb_arguments; i++) {
	argument = (argument_t *)offset;
	argument_size = packet->arguments[i].size;
	argument->argument_size = htonl(argument_size);
	offset += sizeof(argument_t);
	memmove(offset, packet->arguments[i].data, argument_size);
	offset += argument_size;
    }

    return amprotocol_write(protocol->fd, buffer, block_size);
}

ssize_t
amprotocol_send_list(
    amprotocol_t *protocol,
    int           cmd,
    int           nb_arguments,
    ...)
{
    command_t   *command;
    argument_t  *argument;
    char        *buffer, *offset;
    int          block_size;
    int          i;
    int          argument_size;
    va_list      arguments_list;
    char       **arguments;

    block_size = sizeof(command_t);

    if (cmd >= CMD_MAX) {
	errno = EINVAL;
	return -1;
    }
    if ((uint32_t)nb_arguments != find_number_of_args(protocol, cmd)) {
	errno = EINVAL;
	return -1;
    }
    arguments = malloc(nb_arguments * sizeof(char *));
    va_start(arguments_list, nb_arguments);
    for (i=0; i<nb_arguments; i++) {
	arguments[i] = va_arg(arguments_list, char *);
	block_size += sizeof(argument_t) + strlen(arguments[i]) + 1;
    }
    va_end(arguments_list);

    buffer = offset = malloc(block_size);
    command = (command_t *)offset;
    command->magic = htons(protocol->magic);
    command->command = htons(cmd);
    command->block_size = htonl(block_size);
    command->nb_arguments = htonl(nb_arguments);
    offset += sizeof(command_t);
    for (i=0; i<nb_arguments; i++) {
	argument = (argument_t *)offset;
	argument_size = strlen(arguments[i]) + 1;
	argument->argument_size = htonl(argument_size);
	offset += sizeof(argument_t);
	memmove(offset, arguments[i], argument_size);
	offset += argument_size;
    }

    return amprotocol_write(protocol->fd, buffer, block_size);
}

ssize_t
amprotocol_send_binary(
    amprotocol_t *protocol,
    int           cmd,
    int           nb_arguments,
    ...)
{
    command_t   *command;
    argument_t  *argument;
    char        *buffer, *offset;
    int          block_size;
    int          i;
    va_list      arguments_list;
    char       **arguments;
    int         *sizes;

    block_size = sizeof(command_t);

    if (cmd >= CMD_MAX) {
	errno = EINVAL;
	return -1;
    }
    if ((uint32_t)nb_arguments != find_number_of_args(protocol, cmd)) {
	errno = EINVAL;
	return -1;
    }
    arguments = malloc(nb_arguments * sizeof(char *));
    sizes = malloc(nb_arguments * sizeof(int));
    va_start(arguments_list, nb_arguments);
    for (i=0; i<nb_arguments; i++) {
	sizes[i] = va_arg(arguments_list, int);
	arguments[i] = va_arg(arguments_list, char *);
	block_size += sizeof(argument_t) + sizes[i];
    }
    va_end(arguments_list);

    buffer = offset = malloc(block_size);
    command = (command_t *)offset;
    command->magic = htons(protocol->magic);
    command->command = htons(cmd);
    command->block_size = htonl(block_size);
    command->nb_arguments = htonl(nb_arguments);
    offset += sizeof(command_t);
    for (i=0; i<nb_arguments; i++) {
	argument = (argument_t *)offset;
	argument->argument_size = htonl(sizes[i]);
	offset += sizeof(argument_t);
	memmove(offset, arguments[i], sizes[i]);
	offset += sizes[i];
    }

    free(arguments);
    free(sizes);
    return amprotocol_write(protocol->fd, buffer, block_size);
}

amprotocol_packet_t *
amprotocol_get(
    amprotocol_t *protocol)
{
    amprotocol_packet_t *packet;
    command_t            command;
    argument_t          *argument;
    char                *buffer, *offset;
    int                  argument_size;
    ssize_t              size;
    uint32_t             i;
    int                  save_errno;

    size = amprotocol_read(protocol->fd, &command, sizeof(command_t));
    save_errno = errno;
    if (size != sizeof(command_t)) {
	errno = save_errno;
	if (size != -1)
	    errno = ENODATA;
	return NULL;
    }
    packet = malloc(sizeof(amprotocol_packet_t));
    packet->magic = ntohs(command.magic);
    if (packet->magic != protocol->magic) {
	errno = EINVAL;
	return NULL;
    }
    packet->command = ntohs(command.command);
    packet->block_size = ntohl(command.block_size);
    packet->nb_arguments = ntohl(command.nb_arguments);

    if (packet->nb_arguments != find_number_of_args(protocol, packet->command)) {
	errno = EINVAL;
	return NULL;
    }
    if (packet->nb_arguments > 0) {
	buffer = malloc(packet->block_size - sizeof(command_t));
	size = amprotocol_read(protocol->fd, buffer, packet->block_size - sizeof(command_t));
	if (size != (ssize_t)(packet->block_size - sizeof(command_t))) {
	    int save_errno = errno;
	    free(packet);
	    errno = save_errno;
	    if (size != -1)
		errno = ENODATA;
	    return NULL;
	}
	offset = buffer;
	packet->arguments = malloc(packet->nb_arguments * sizeof(an_argument_t));
	for (i=0; i < packet->nb_arguments; i++) {
	    argument = (argument_t *)offset;
	    argument_size = ntohl(argument->argument_size);
	    offset += sizeof(argument_t);
	    packet->arguments[i].size = argument_size;
	    packet->arguments[i].data = malloc(argument_size);
	    memmove(packet->arguments[i].data, offset, argument_size);
	    offset += argument_size;
	}
    } else {
	packet->arguments = NULL;
    }

    return packet;
}

amprotocol_packet_t *
amprotocol_parse(
    amprotocol_t *protocol,
    char         *buf_data,
    size_t        len)
{
    amprotocol_packet_t *packet;
    command_t           *command;
    argument_t          *argument;
    char                *offset;
    int                  argument_size;
    uint32_t             i;
    char                *p = buf_data;

    if (len < sizeof(command_t)) {
	errno = ENODATA;
	return NULL;
    }
    command = (command_t *)p;
    len -= sizeof(command_t);
    p   += sizeof(command_t);
    packet = malloc(sizeof(amprotocol_packet_t));
    packet->magic = ntohs(command->magic);
    if (packet->magic != protocol->magic) {
	errno = EINVAL;
	return NULL;
    }
    packet->command = ntohs(command->command);
    packet->block_size = ntohl(command->block_size);
    packet->nb_arguments = ntohl(command->nb_arguments);

    if (packet->nb_arguments != find_number_of_args(protocol, packet->command)) {
	//fprintf(stderr, "application_protocol_parse: bad number of argument (%d) for command %d\n", packet->nb_arguments, packet->command);
	errno = EINVAL;
	return NULL;
    }
    if (packet->nb_arguments > 0) {
	if (len < packet->block_size - sizeof(command_t)) {
	    free(packet);
	    errno = ENODATA;
	    return NULL;
	}
	offset = p;
	len -= packet->block_size - sizeof(command_t);
	p   += packet->block_size - sizeof(command_t);
	packet->arguments = malloc(packet->nb_arguments * sizeof(an_argument_t));
	for (i=0; i < packet->nb_arguments; i++) {
	    argument = (argument_t *)offset;
	    argument_size = ntohl(argument->argument_size);
	    offset += sizeof(argument_t);
	    packet->arguments[i].size = argument_size;
	    packet->arguments[i].data = malloc(argument_size);
	    memmove(packet->arguments[i].data, offset, argument_size);
	    offset += argument_size;
	}
    } else {
	packet->arguments = NULL;
    }

    return packet;
}

void
free_amprotocol_packet(
    amprotocol_packet_t *packet)
{
    uint32_t i;

    if (!packet)
	return;

    for (i=0; i < packet->nb_arguments; i++) {
	free(packet->arguments[i].data);
    }
    free(packet->arguments);
    free(packet);
}

