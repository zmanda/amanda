#! @PERL@
use lib '@amperldir@';
use strict;
use Amanda::Device qw( :ReadLabelStatusFlags );
use Amanda::Debug qw( dbopen set_pname :logging );
use Amanda::Config qw( config_init :config_init_flags :confparm_key);

# try to open the device and read its label, returning the device_read_label
# result (one or more of ReadLabelStatusFlags)
sub try_read_label {
    my ($device_name) = @_;

    if ( !$device_name ) {
        return $READ_LABEL_STATUS_DEVICE_MISSING;
    }

    my $device = Amanda::Device->new($device_name);
    if ( !$device ) {
        return $READ_LABEL_STATUS_DEVICE_MISSING
            | $READ_LABEL_STATUS_DEVICE_ERROR;
    }

    $device->set_startup_properties_from_config();

    return $device->read_label();
}

# print the results, one flag per line
sub print_result {
    my ($flags) = @_;

    print join( "\n", ReadLabelStatusFlags_to_strings($flags) ), "\n";
}

## Application initialization

# TODO: gettext
# TODO: safe_cd, safe_fds
set_pname("amdevcheck");
dbopen("server");

## Argument processing

sub usage {
    print <<EOF;
Usage: amdevcheck <config> [ <device name> ]
EOF
    exit(1);
}

usage() if ( $#ARGV < 0 || $#ARGV > 1 );

my $config_name = $ARGV[0];
if (!Amanda::Config::config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name)) {
    g_critical("errors processing config file \"$Amanda::Config::config_filename\"");
}

# TODO: check_running_as

## Check the device

my $device_name;
if ( $#ARGV == 1 ) {
    $device_name = $ARGV[1];
}
else {
    $device_name = Amanda::Config::getconf($CNF_TAPEDEV);
}

print_result( try_read_label($device_name) );
