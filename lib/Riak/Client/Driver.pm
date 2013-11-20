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

sub _build_connector {
    Riak::Client::Connector->new( socket => shift()->socket );
}

sub perform_request {
    my ( $self, $request_code, $request_body ) = @_;
    $self->connector->perform_request(
      pack( 'c a*', $request_code, $request_body )
    );
}

sub read_response {
    my ($self)   = @_;
    my $response = $self->connector->read_response()
      or return { code => -1,
                  body => undef,
                  error => $ERRNO || "Socket Closed" };
    my ( $code, $body ) = unpack( 'c a*', $response );
    { code => $code, body => $body, error => undef };
}

1;

__END__

=head1 DESCRIPTION
  
  Internal class
