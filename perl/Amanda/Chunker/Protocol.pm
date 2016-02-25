# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

=head1 NAME

Amanda::Chunker::Protocol

=head1 DESCRIPTION

This package is a component of the Amanda chunker, and is not intended for use
by other scripts or applications.

This package define the protocol between the chunker and the driver, it is
used by the chunker.

=cut

use strict;
use warnings;

package Amanda::Chunker::Protocol;

use Amanda::IPC::LineProtocol;
use base "Amanda::IPC::LineProtocol";

use constant START => message("START",
    format => [ qw( timestamp ) ],
);

use constant PORT_WRITE => message("PORT-WRITE",
    format => [ qw( handle filename hostname features diskname level datestamp
	    chunk_size progname use_bytes options ) ],
);

use constant FAILED => message("FAILED",
    format => {
	in => [ qw( handle ) ],
	out => [ qw( handle msg ) ],
    },
);

use constant DONE => message("DONE",
    format => {
	in => [ qw( handle client_crc ) ],
	out => [ qw( handle size server_crc stats ) ],
    },
);

use constant QUIT => message("QUIT",
    on_eof => 1,
);


use constant PARTIAL => message("PARTIAL",
    format => [ qw( handle size server_crc stats ) ],
);

use constant PORT => message("PORT",
    format => [ qw( handle port ipport ) ],
);

use constant NO_ROOM => message("NO-ROOM",
    format => [ qw( handle use ) ],
);

use constant RQ_MORE_DISK => message("RQ-MORE-DISK",
    format => [ qw( handle ) ],
);

use constant BAD_COMMAND => message("BAD-COMMAND",
    format => [ qw( message ) ],
);

use constant ABORT => message("ABORT",
    format => [ qw( handle ) ],
);

use constant ABORT_FINISHED => message("ABORT-FINISHED",
    format => [ qw( handle ) ],
);

use constant DUMPER_STATUS => message("DUMPER-STATUS",
    format => [ qw( handle ) ],
);

use constant CONTINUE => message("CONTINUE",
    format => [ qw( handle filename chunk_size use_bytes ) ],
);

1;
