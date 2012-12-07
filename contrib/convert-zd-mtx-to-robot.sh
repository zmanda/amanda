#! /bin/sh

if test $# -ne 1; then
    echo "USAGE: $0 <CONFIG>"
    exit 1
fi

CONFIG=$1
CFGDIR=`amgetconf build.CONFIG_DIR`
if test -z "$CFGDIR"; then
    echo "could not get CONFIG_DIR - is Amanda set up correctly?"
    exit 1;
fi

# cd into config dir so relative paths work
cd "$CFGDIR/$CONFIG"

##
# get the configuration (this is copied directly from chg-zd-mtx)

die() {
    echo "$*"
    exit 1
}

changerfile=`amgetconf $CONFIG changerfile 2>/dev/null`
if [ -z "$changerfile" ]; then
     die "changerfile must be specified in amanda.conf"
fi
conf_match=`expr "$changerfile" : .\*\.conf\$`
if [ $conf_match -ge 6 ]; then
	configfile=$changerfile
	changerfile=`echo $changerfile | sed 's/.conf$//g'`
else
	configfile=$changerfile.conf
fi
if [ ! -e $configfile ]; then
    die "configuration file \"$configfile\" doesn't exist"
fi
if [ ! -f $configfile ]; then
    die "configuration file \"$configfile\" is not a file"
fi
cleanfile=$changerfile-clean
accessfile=$changerfile-access
slotfile=$changerfile-slot
labelfile=$changerfile-barcodes

varlist=
varlist="$varlist firstslot"
varlist="$varlist lastslot"
varlist="$varlist cleanslot"
varlist="$varlist cleancycle"
varlist="$varlist OFFLINE_BEFORE_UNLOAD"	# old name
varlist="$varlist offline_before_unload"
varlist="$varlist unloadpause"
varlist="$varlist AUTOCLEAN"			# old name
varlist="$varlist autoclean"
varlist="$varlist autocleancount"
varlist="$varlist havereader"
varlist="$varlist driveslot"
varlist="$varlist poll_drive_ready"
varlist="$varlist initial_poll_delay"
varlist="$varlist max_drive_wait"
varlist="$varlist slotinfofile"

for var in $varlist
do
	val="`cat $configfile 2>/dev/null | sed -n '
# Ignore comment lines (anything starting with a #).
/^[ 	]*#/d
# Find the first var=val line in the file, print the value and quit.
/^[ 	]*'$var'[ 	]*=[ 	]*\([^ 	][^ 	]*\).*/	{
	s/^[ 	]*'$var'[ 	]*=[ 	]*\([^ 	][^ 	]*\).*/\1/p
	q
}
'`"
	eval $var=\"$val\"
done

firstslot=${firstslot:-'-1'}				# default: mtx status
lastslot=${lastslot:-'-1'}				# default: mtx status
driveslot=${driveslot:-'0'}				# default: 0
cleanslot=${cleanslot:-'-1'}				# default: -1
cleancycle=${cleancycle:-'120'}				# default: two minutes
if [ -z "$offline_before_unload" -a -n "$OFFLINE_BEFORE_UNLOAD" ]; then
	offline_before_unload=$OFFLINE_BEFORE_UNLOAD	# (old name)
fi
offline_before_unload=${offline_before_unload:-'0'}	# default: 0
unloadpause=${unloadpause:-'0'}				# default: 0
if [ -z "$autoclean" -a -n "$AUTOCLEAN" ]; then
	autoclean=$AUTOCLEAN				# (old name)
fi
autoclean=${autoclean:-'0'}				# default: 0
autocleancount=${autocleancount:-'99'}			# default: 99
havereader=${havereader:-'0'}				# default: 0
poll_drive_ready=${poll_drive_ready:-'3'}		# default: three seconds
initial_poll_delay=${initial_poll_delay:-'0'}		# default: zero zeconds
max_drive_wait=${max_drive_wait:-'120'}			# default: two minutes

tapedev=`amgetconf $CONFIG tapedev 2>/dev/null`
if [ -z "$tapedev" ]; then
    die "tapedev may not be empty"
fi

changerdev=`amgetconf $CONFIG changerdev 2>/dev/null`
if [ -z "$changerdev" ]; then
    die "changerdev may not be empty"
elif [ $changerdev = "/dev/null" ]; then
    die "changerdev ($changerdev) may not be the null device"
fi

####
# Create the chg-robot config

# note that cleaning is not supported by chg-robot, so the cleaning parameters are
# not converted.

showbool() {
    if test "$1" -ne 0; then
	echo 'yes'
    else
	echo 'no'
    fi
}

echo "Remove the following parameters, if present, from your amanda.conf:"
echo "   tpchanger changerfile changerdev tapedev"
echo ""
echo "Add the following to amanda.conf (you may want to use a more descriptive name for the changer):"
echo ""
echo "define changer \"converted-from-chg-zd-mtx\" {"
echo "  tpchanger \"chg-robot:$changerdev\""
echo "  changerfile \"$changerfile-state\""
echo "  property \"tape-device\" \"$driveslot=$tapedev\""
echo "  property \"eject-before-unload\" \""`showbool $offline_before_unload`"\""
if test $firstslot -ge 0 && test $lastslot -ge 0; then
    echo "  property \"use-slots\" \"$firstslot-$lastslot\""
elif test $firstslot -ge 0 || test $lastslot -ge 0; then
    echo "  # you may want to set the USE-SLOTS property appropriately"
fi
if test "${unloadpause:-0}" -gt 0; then
    echo "  property \"eject-delay\" \"$unloadpause s\""
fi
if test $havereader -eq 0; then
    echo "  property \"fast-search\" \"no\""
    echo "  property \"ignore-barcodes\" \"yes\""
fi
echo "  property \"load-poll\" \"$initial_poll_delay s poll $poll_drive_ready s until $max_drive_wait s\""
echo "}"
echo "tpchanger \"converted-from-chg-zd-mtx\""
echo ""

####
# and offer suggestions for loading metadata, if slotinfofile is set

if test -z "$slotinfofile"; then
    echo "Once this configuration is in place, run"
    echo "  amtape $CONFIG update"
    echo "to update the changer's tape database.  For a large changer, this may take some time."
else
    echo "Run the following commands to initialize the changer's tape database."
    cat "$slotinfofile" | while read slot label; do
	echo "  amtape $CONFIG update \"$slot=$label\""
    done
fi
