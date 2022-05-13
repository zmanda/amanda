#!@PERL@

use POSIX;

my $buf;
my $count = 0;
while ((my $size = POSIX::read(0, $buf, 32768)) > 0) {
    POSIX::write(1, $buf, $size);
    $count += $size;
    if ($count >= 65536) {
	print STDERR "amcat-error-no-exec: failure X\n";
	exit 1;
    }
}

print STDERR "amcat-error-no-exec: failure Y\n";
exit 1;
