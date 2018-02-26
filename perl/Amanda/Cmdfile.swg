/*
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
 * Contact information: Carbonite Inc., 756 N Pastoria Ave
 * Sunnyvale, CA 94085, or: http://www.zmanda.com
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
    CMD_FLUSH,
    CMD_RESTORE
} cmdoperation_t;

typedef enum cmdstatus_e {
    CMD_DONE,
    CMD_TODO,
    CMD_PARTIAL
} cmdstatus_t;

%{
typedef cmddata_t Cmddata;
%}

typedef struct {
    /* change here must also be done to 'sub new_Cmddata' */
    int             id;
    cmdoperation_t  operation;
    char           *config;
    char           *src_storage;
    char           *src_pool;
    char           *src_label;
    int             src_fileno;
    char           *src_labels_str;
    GSList         *src_labels;
    char           *holding_file;
    char           *hostname;
    char           *diskname;
    char           *dump_timestamp;
    int             level;
    char           *dst_storage;
    pid_t           working_pid;
    cmdstatus_t     status;
    int             todo;
    off_t           size;
    time_t          start_time;
    time_t          expire;
    int             count;

    %extend {
	/* disable destructor */
	/* they are freed when closing the cmddatas */
	~Cmddata() {
	    return;
	}

    }
} Cmddata;

%{
typedef cmddatas_t Cmdfile;
%}

typedef struct {
    int        version;
    int        max_id;
    file_lock *lock;
    cmdfile_t  cmdfile;

    %extend {
	/* constructor */
	Cmdfile(char *filename) {
	    return read_cmdfile(filename);
	}

	/* destructor */
	~Cmdfile() {
	    return close_cmdfile(self);
	}

	void write() {
	    return write_cmdfile(self);
	}

	void unlock() {
	    return unlock_cmdfile(self);
	}

	int add_to_memory(Cmddata *cmddata) {
	    return add_cmd_in_memory(self, cmddata);
	}

	void add_to_file(Cmddata *cmddata) {
	    add_cmd_in_cmdfile(self, cmddata);
	}

	gboolean has_holding(char *holding_file) {
	    return holding_in_cmdfile(self, holding_file);
	}

	char *get_ids_for_holding(char *holding_file) {
	    return cmdfile_get_ids_for_holding(self, holding_file);
	}

	void remove_for_restore_label(char *hostname, char *diskname,
				      char *timestamp, char *storage,
				      char *pool, char *label) {
	    cmdfile_remove_for_restore_label(self, hostname, diskname,
					     timestamp, storage, pool, label);
	}


	void remove_for_restore_holding(char *hostname, char *diskname,
					char *timestamp, char *holding_file) {
	    cmdfile_remove_for_restore_holding(self, hostname, diskname,
					       timestamp, holding_file);
	}

	%typemap(out) cmdfile_glist_t {
	    GList *aa;

	    int countx = 0;
	    for (aa = $1; aa != NULL; aa = aa->next) {
		countx++;
	    }
	    EXTEND(SP, countx);
	    for (aa = $1; aa != NULL; aa = aa->next) {
		Cmddata *data = (Cmddata *)aa->data;
		HV *hv = newHV();
		SV *rv = newRV_noinc((SV *)hv);
		g_debug("id: %d", data->id);
		hv_store(hv, "id"            , 2 , newSViv(data->id), 0);
		hv_store(hv, "operation"     , 9 , newSViv(data->operation), 0);
		hv_store(hv, "config"        , 6 , newSVpv(data->config, 0), 0);
		hv_store(hv, "src_storage"   , 11, newSVpv(data->src_storage, 0), 0);
		hv_store(hv, "src_pool"      , 8 , newSVpv(data->src_pool, 0), 0);
		hv_store(hv, "src_label"     , 9 , newSVpv(data->src_label, 0), 0);
		hv_store(hv, "src_fileno"    , 10, newSViv(data->src_fileno), 0);
		hv_store(hv, "src_labels_str", 14, newSVpv(data->src_labels_str, 0), 0);
		hv_store(hv, "holding_file"  , 12, newSVpv(data->holding_file, 0), 0);
		hv_store(hv, "hostname"      , 8 , newSVpv(data->hostname, 0), 0);
		hv_store(hv, "diskname"      , 8 , newSVpv(data->diskname, 0), 0);
		hv_store(hv, "dump_timestamp", 14, newSVpv(data->dump_timestamp, 0), 0);
		hv_store(hv, "level"         , 5 , newSViv(data->level), 0);
		hv_store(hv, "dst_storage"   , 11, newSVpv(data->dst_storage, 0), 0);
		hv_store(hv, "working_pid"   , 11, newSViv(data->working_pid), 0);
		hv_store(hv, "status"        , 6 , newSViv(data->status), 0);
		hv_store(hv, "todo"          , 4 , newSViv(data->todo), 0);
		hv_store(hv, "size"          , 4 , newSViv(data->size), 0);
		hv_store(hv, "start_time"    , 10, newSViv(data->start_time), 0);
		hv_store(hv, "expire"        , 6 , newSViv(data->expire), 0);
		hv_store(hv, "count"         , 5 , newSViv(data->count), 0);
		$result = sv_2mortal(rv);
		argvi++;
	    }
	    g_list_free($1);
	}
	cmdfile_glist_t get_all() {
	    return cmdfile_get_all_glist(self);
	}

	%typemap(out) cmdfile_t;

    }
} Cmdfile;


%perlcode %{
# SWIG produces a sub-package for the cmddatas_t "class", in this case named
# Amanda::Cmdfile::cmddatas_t.  For user convenience, we allow
# Amanda::Cmdfile->new(..) to do the same thing.  This is a wrapper function,
# and not just a typeglob assignment, because we want to get the right blessing.
package Amanda::Cmdfile;
sub new {
    my $pkg = shift;
    Amanda::Cmdfile::Cmdfile->new(@_);
}

sub new_Cmddata {
    my $class = shift;
    my %params = @_;

    $params{'working_pid'} ||= 0;
    $params{'todo'} ||= 0;
    $params{'size'} ||= 0;

    my $cmddata = Amanda::Cmdfile::Cmddata->new();
    $cmddata->{'operation'}      = $params{'operation'};
    $cmddata->{'config'}         = $params{'config'} if defined $params{'config'};
    $cmddata->{'src_storage'}    = $params{'src_storage'} if defined $params{'src_storage'};
    $cmddata->{'src_pool'}       = $params{'src_pool'} if defined $params{'src_pool'};
    $cmddata->{'src_label'}      = $params{'src_label'} if defined $params{'src_label'};
    $cmddata->{'src_fileno'}     = $params{'src_fileno'} if defined $params{'src_fileno'};
    $cmddata->{'src_labels_str'} = $params{'src_labels_str'} if defined $params{'src_labels_str'};
    $cmddata->{'src_labels'}     = $params{'src_labels'} if defined $params{'src_labels'};
    $cmddata->{'holding_file'}   = $params{'holding_file'} if defined $params{'holding_file'};
    $cmddata->{'hostname'}       = $params{'hostname'} if defined $params{'hostname'};
    $cmddata->{'diskname'}       = $params{'diskname'} if defined $params{'diskname'};
    $cmddata->{'dump_timestamp'} = $params{'dump_timestamp'} if defined $params{'dump_timestamp'};
    $cmddata->{'label'}          = $params{'label'} if defined $params{'label'};
    $cmddata->{'dst_storage'}    = $params{'dst_storage'} if defined $params{'dst_storage'};
    $cmddata->{'working_pid'}    = $params{'working_pid'} if defined $params{'working_pid'};
    $cmddata->{'status'}         = $params{'status'} if defined $params{'status'};
    $cmddata->{'todo'}           = $params{'todo'} if defined $params{'todo'};
    $cmddata->{'size'}           = $params{'size'} if defined $params{'size'};
    $cmddata->{'start_time'}     = $params{'start_time'} if defined $params{'start_time'};

    return $cmddata;
};

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

