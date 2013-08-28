## no critic (RequireUseStrict, RequireUseWarnings)
package Riak::Client::Timeout;
## use critic

use Moo::Role;

# ABSTRACT: socket interface to add timeout in in/out operations

requires 'sysread';
requires 'syswrite';

1;

__END__

=head1 DESCRIPTION
  
  Internal class
