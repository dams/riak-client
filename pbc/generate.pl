use strict;
use warnings;
use FindBin qw($Bin);
use Google::ProtocolBuffers;

Google::ProtocolBuffers->parsefile(
    "$Bin/riak.proto",
    {   generate_code    => "$Bin/../lib/Riak/Client/PBC.pm",
        create_accessors => 1
    }
);
