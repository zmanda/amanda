# Copyright (c) 2009-2012 Zmanda Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
#
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

=head1 NAME

Amanda::Taper::Protocol

=head1 DESCRIPTION

This package is a component of the Amanda taper, and is not intended for use by
other scripts or applications.

This package define the protocol between the taper and the driver, it is
used by L<Amanda::Taper::Controller> and L<Amanda::Taper::Worker>

=cut

use lib '@amperldir@';
use strict;
use warnings;

package Amanda::Taper::Protocol;

use Amanda::IPC::LineProtocol;
use base "Amanda::IPC::LineProtocol";

use constant START_TAPER => message("START-TAPER",
    format => [ qw( worker_name timestamp ) ],
);

use constant PORT_WRITE => message("PORT-WRITE",
    format => [ qw( worker_name handle hostname diskname level datestamp
	    dle_tape_splitsize dle_split_diskbuffer dle_fallback_splitsize dle_allow_split
	    part_size part_cache_type part_cache_dir part_cache_max_size
	    data_path ) ],
);

use constant FILE_WRITE => message("FILE-WRITE",
    format => [ qw( worker_name handle filename hostname diskname level datestamp
	    dle_tape_splitsize dle_split_diskbuffer dle_fallback_splitsize dle_allow_split
	    part_size part_cache_type part_cache_dir part_cache_max_size
	    orig_kb) ],
);

use constant START_SCAN => message("START-SCAN",
    format => [ qw( worker_name handle ) ],
);

use constant NEW_TAPE => message("NEW-TAPE",
    format => {
	in => [ qw( worker_name handle ) ],
	out => [ qw( handle label ) ],
    },
);

use constant NO_NEW_TAPE => message("NO-NEW-TAPE",
    format => {
	in => [ qw( worker_name handle reason ) ],
	out => [ qw( handle ) ],
    }
);

use constant FAILED => message("FAILED",
    format => {
	in => [ qw( worker_name handle ) ],
	out => [ qw( handle input taper inputerr tapererr ) ],
    },
);

use constant DONE => message("DONE",
    format => {
	in => [ qw( worker_name handle orig_kb ) ],
	out => [ qw( handle input taper stats inputerr tapererr ) ],
    },
);

use constant QUIT => message("QUIT",
    on_eof => 1,
);

use constant TAPER_OK => message("TAPER-OK",
    format => [ qw( worker_name ) ],
);

use constant TAPE_ERROR => message("TAPE-ERROR",
    format => [ qw( worker_name message ) ],
);

use constant PARTIAL => message("PARTIAL",
    format => [ qw( handle input taper stats inputerr tapererr ) ],
);

use constant PARTDONE => message("PARTDONE",
    format => [ qw( handle label fileno kb stats ) ],
);

use constant REQUEST_NEW_TAPE => message("REQUEST-NEW-TAPE",
    format => [ qw( handle ) ],
);

use constant PORT => message("PORT",
    format => [ qw( worker_name handle port ipports ) ],
);

use constant BAD_COMMAND => message("BAD-COMMAND",
    format => [ qw( message ) ],
);

use constant TAKE_SCRIBE_FROM => message("TAKE-SCRIBE-FROM",
    format => [ qw( worker_name handle from_worker_name) ],
);

use constant DUMPER_STATUS => message("DUMPER-STATUS",
    format => [ qw( handle ) ],
);

use constant CLOSE_VOLUME => message("CLOSE-VOLUME",
    format => [ qw( worker_name ) ],
);

1;
