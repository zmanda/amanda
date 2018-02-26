#!/bin/sh


set -v

#
# Show the build info for NDMJOB
#
ndmjob -v


#
# Query the DATA and TAPE agents, force NDMPv2 and NDMv3
#
ndmjob -q -D./2
ndmjob -q -D./3

ndmjob -q -T./2
ndmjob -q -T./3

# ndmjob -q -Tconexus/2,,
# ndmjob -q -Tconexus/3,,


#
# Set up FakeTape, a disk file used by the
# tape simulator.
#
rm -f /usr/tmp/FakeTape FakeTape
touch /usr/tmp/FakeTape
ln -s /usr/tmp/FakeTape FakeTape


#
# Test the TAPE agent
#

ndmjob -o test-tape  -T. -fFakeTape -v
ndmjob -o test-mover -T. -fFakeTape -v


#
# Label the tape, read back
#
ndmjob -o init-labels -T. -fFakeTape -mMyTape
ndmjob -l -T. -fFakeTape


#
# Make sure tape label checking is working
#
ndmjob -c -D. -C /tmp/tough -I so-c.nji -fFakeTape -mMyTapeXXX

#
# Make a backup of the "tough" directory tree
#
ndmjob -cvv -D. -C /tmp/tough -I so-c.nji -fFakeTape -mMyTape
wc so-c.nji

#
# Recover file history using sequential access method
#
ndmjob -tvv -D. -I so-t-seq.nji -fFakeTape -mMyTape -E DIRECT=No
wc so-t-seq.nji
diff so-c.nji so-t-seq.nji

#
# Recover file history using direct access method
#
ndmjob -tvv -D. -I so-t.dir.nji -fFakeTape -mMyTape -E DIRECT=Yes
wc sso-t-dir.nji
diff so-c.nji so-t-dir.nji


#
# Recover entire backup
#
rm -rf /tmp/tough-x
mkdir /tmp/tough-x
ndmjob -xvv -D. -C /tmp/tough-x -fFakeTape -mMyTape
diff -r /tmp/tough /tmp/tough-x

