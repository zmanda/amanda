#!@PERL@

use POSIX;

my $buf;
my $count = 0;
while ((my $size = POSIX::read(0, $buf, 32768)) > 0) {
    POSIX::write(1, $buf, $size);
}

print STDERR "amcat-error-end: failure Y\n";
exit 1;
