package Kage::Backend;
use Moo;

has host => (
    is => 'ro',
    required => 1,
);

has port => (
    is => 'ro',
    required => 1,
);

has name => (
    is => 'ro',
    required => 1,
);

no Moo;

1;
