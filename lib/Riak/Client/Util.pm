## no critic (RequireUseStrict, RequireUseWarnings)
package Riak::Client::Util;
## use critic
use Config;
use Exporter 'import';

@EXPORT_OK = qw(is_windows is_netbsd is_solaris);
#ABSTRACT: util class, provides is_windows, is_solaris, etc

sub is_windows {
    $Config{osname} eq 'MSWin32';
}

sub is_netbsd {
    $Config{osname} eq 'netbsd';
}

sub is_solaris {
    $Config{osname} eq 'solaris';
}

1;

=head1 DESCRIPTION
  
  Internal class
