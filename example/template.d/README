README for template.d directory
-------------------------------

This directory contains seven files that are used by amserverconfig.

amanda-harddisk.conf: 		contains common parameters for a typical Amanda configuration using
				virtual tapes.

amanda-single-tape.conf:	contains common parameters for a typical Amanda configuration using
				one tape.

amanda-tape-changer.conf:	contains common parameters for a typical Amanda configuration using
				tape changer.

advanced.conf:			contains advanced Amanda parameters that are not usually changed by a site.

dumptypes:			contains Amanda dumptype definitions.

tapetypes:			contains Amanda tapetype definitions.

README:				this file.


amserverconfig will create a Amanda configuration in the following directory structure:

/etc/amanda/template.d:
dumptypes
tapetypes

/etc/amanda/$config_name:
amanda.conf	[ it's a copy of amanda-harddisk.conf, amanda-single-tape.conf or amanda-tape-changer.conf ] 
advanced.conf


See amserverconfig(8) or amaddclient(8) for detail.



