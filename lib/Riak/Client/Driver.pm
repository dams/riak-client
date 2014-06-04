## no critic (RequireUseStrict, RequireUseWarnings)
package Riak::Client::Driver;
## use critic

use English qw( -no_match_vars );
use Riak::Client::Connector;
use Moo;
use Types::Standard -types;

# ABSTRACT: Riak Driver, deal with the binary protocol

has socket => ( is => 'ro');
has connector => ( is => 'lazy');



1;

__END__

=head1 DESCRIPTION
  
  Internal class
