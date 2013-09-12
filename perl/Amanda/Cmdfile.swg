/*
 * Copyright (c) 2013 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

%module "Amanda::Cmdfile"
%include "amglue/amglue.swg"
%include "exception.i"
%import "Amanda/Config.swg"

%include "Amanda/Cmdfile.pod"

%{
#include "conffile.h"
#include "cmdfile.h"
#include "amglue.h"
%}

%perlcode %{
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :getconf config_dir_relative );
use Amanda::Util qw ( match_disk match_host );
%}

typedef enum cmdoperation_e {
    CMD_COPY,
    CMD_FLUSH
} cmdoperation_t;

typedef enum cmdstatus_e {
    CMD_DONE,
    CMD_TODO,
    CMD_PARTIAL
} cmdstatus_t;

%rename(Cmddata) cmddata_t;
typedef struct cmddata_s {
    int             id;
    cmdoperation_t  operation;
    char           *config;
    char           *storage;
    char           *pool;
    char           *label;
    char           *holding_file;
    char           *hostname;
    char           *diskname;
    char           *dump_timestamp;
    char           *storage_dest;
    pid_t           working_pid;
    cmdstatus_t     status;
    int             todo;
    off_t           size;

    %extend {
	/* disable destructor */
	/* they are freed when closing the cmddatas */
	~cmddata_t() {
	    return;
	}

    }
} cmddata_t;

%perlcode %{

package Amanda::Cmdfile::Cmddata;

sub new {
    my $class = shift;
    my %params = @_;

    $params{'working_pid'} ||= 0;
    $params{'todo'} ||= 0;
    $params{'size'} ||= 0;

    my $cmddata = Amanda::Cmdfile::cmddata_t->new_Cmddata();
    $cmddata->{'operation'}      = $params{'operation'};
    $cmddata->{'config'}         = $params{'config'};
    $cmddata->{'storage'}        = $params{'storage'};
    $cmddata->{'pool'}           = $params{'pool'};
    $cmddata->{'label'}          = $params{'label'};
    $cmddata->{'holding_file'}   = $params{'holding_file'};
    $cmddata->{'hostname'}       = $params{'hostname'};
    $cmddata->{'diskname'}       = $params{'diskname'};
    $cmddata->{'dump_timestamp'} = $params{'dump_timestamp'};
    $cmddata->{'storage_dest'}   = $params{'storage_dest'};
    $cmddata->{'working_pid'}    = $params{'working_pid'};
    $cmddata->{'status'}         = $params{'status'};
    $cmddata->{'todo'}           = $params{'todo'};
    $cmddata->{'size'}           = $params{'size'};

    return $cmddata;
};

%}

%rename(Cmdfile) cmddatas_t;
typedef struct cmdatas_s {
    int        version;
    int        max_id;
    file_lock *lock;
    cmdfile_t  cmdfile;

    %extend {
	/* constructor */
	cmddatas_t(char *filename) {
	    return read_cmdfile(filename);
	}

	/* destructor */
	~cmddatas_t() {
	    return close_cmdfile(self);
	}

	void write() {
	    return write_cmdfile(self);
	}

	void unlock() {
	    return unlock_cmdfile(self);
	}

	int add_to_memory(cmddata_t *cmddata) {
	    return add_cmd_in_memory(self, cmddata);
	}

	void add_to_file(cmddata_t *cmddata) {
	    add_cmd_in_cmdfile(self, cmddata);
	}

	gboolean has_holding(char *holding_file) {
	    return holding_in_cmdfile(self, holding_file);
	}

	char *get_ids_for_holding(char *holding_file) {
	    return cmdfile_get_ids_for_holding(self, holding_file);
	}
    }
} cmddatas_t;


%perlcode %{
# SWIG produces a sub-package for the cmddatas_t "class", in this case named
# Amanda::Cmdfile::cmddatas_t.  For user convenience, we allow
# Amanda::Cmdfile->new(..) to do the same thing.  This is a wrapper function,
# and not just a typeglob assignment, because we want to get the right blessing.
package Amanda::Cmdfile;
sub new {
    my $pkg = shift;
    Amanda::Cmdfile::cmddatas_t->new_Cmdfile(@_);
}
%}

%perlcode %{
package Amanda::Cmdfile::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 2100000) {
	return "Argument '$self->{'arg'}' cannot be both a host and a disk.";
    }
}
%}

