package Kage;
use Moo;
use AnyEvent;
use AnyEvent::HTTP ();
use AnyEvent::Socket ();
use HTTP::Message::PSGI ();
use HTTP::Request;
use HTTP::Response;
use Log::Minimal;
use Plack::Request;
use Kage::Backend;
use constant DEBUG => !!$ENV{KAGE_DEBUG};
our $VERSION = '0.01';
BEGIN {
    $Log::Minimal::ENV_DEBUG = "KAGE_DEBUG";
    $Log::Minimal::PRINT = sub {
        my ($time, $type, $message) = @_;
        print STDERR "$time [$type] $message\n";
    };
}

has master => (
    is => 'rw'
);

has callbacks => (
    is => 'lazy',
    default => sub { +{} }
);

has backends => (
    is => 'lazy',
    default => sub { +{} }
);

sub on {
    my ($self, $name, $cb) = @_;
    my $list = $self->callbacks->{$name} ||= [];
    push @$list, $cb;
}

sub fire {
    my ($self, $name, @args) = @_;

    my $list = $self->callbacks->{$name} || [];
    my @ret;
    foreach my $cb (@$list) {
        if (DEBUG) {
            debugf("Firing callback %s for hook '%s'", $cb, $name);
        }
        @ret = $cb->(@args);
    }
    return @ret;
}

sub add_master {
    my ($self, $name, %args) = @_;
    $self->master($name);
    $self->add_backend($name, %args);
}

sub add_backend {
    my ($self, $name, %args) = @_;
    $self->backends->{$name} = Kage::Backend->new(name => $name, %args);
}

sub psgi_app {
    my $self = shift;

    return sub {
        my $env = shift;
        return sub {
            my $responder = shift;

            my $preq = Plack::Request->new($env);
            my $hreq = HTTP::Request->new($preq->method, $preq->uri);
            $preq->headers->scan(sub {
                my ($k, $v) = @_;
                $hreq->headers->push_header($k, $v);
            });
            $hreq->content($preq->content);

            my ($backend_names) = $self->fire('select_backend', $hreq);
            if (DEBUG) {
                local $Log::Minimal::AUTODUMP = 1;
                debugf("Backend names: %s", $backend_names);
            }
            my $backends = $self->backends;

            # When all sub-requests are done, call the responder
            my %responses;
            my $main_cv = AE::cv {
                if (DEBUG) {
                    debugf("Received all responses");
                }
                $self->fire(backend_finished => \%responses);
            };

            my $respond_cv = AE::cv {
                $responder->($_[0]->recv);
            };

            foreach my $backend (map { $backends->{$_} } @$backend_names ) {
                my %response = (
                    backend => $backend,
                    request => $hreq,
                    response => undef
                );
                $responses{$backend->name} = \%response;

                $main_cv->begin;
                my $cv = $self->send_backend($backend, $hreq->clone);
                $cv->cb(sub {
                    $response{response} = HTTP::Message::PSGI::res_from_psgi($cv->recv);
                    if ($backend->name eq $self->master) {
                        if (DEBUG) {
                            debugf("Received response from master, returning to client");
                        }
                        $respond_cv->send($cv->recv);
                    }

                    $main_cv->end;
                });
            }
        };
    }
}

sub send_backend {
    my ($self, $backend, $req) = @_;

    if (DEBUG) {
        debugf("Sending %s '%s %s'", $backend->name, $req->method, $req->uri);
    }

    $self->fire(munge_request => ($backend, $req));

    my %headers;
    $req->headers->scan(sub {
        my ($key, $value) = @_;
        $headers{$key} = $value;
    });

    my $cv = AE::cv;
    my $guard; $guard = AnyEvent::HTTP::http_request(
        $req->method,
        $req->uri,
        headers => \%headers,
        persistent => 0,
        tcp_connect => sub {
            # Override tcp_connect so we connect to the specified
            # backend instead of the request url
            my ($host, $port, $connect_cb, $prepare_cb) = @_;
            if (DEBUG) {
                debugf("Connecting to %s => %s:%s", $backend->name, $backend->host, $backend->port);
            }
            return AnyEvent::Socket::tcp_connect(
                $backend->host,
                $backend->port,
                sub {
                    if (DEBUG) {
                        debugf("Connected to %s => %s:%s", $backend->name, $backend->host, $backend->port);
                    }
                    $connect_cb->(@_),
                },
                $prepare_cb
            );
        },
        sub {
            undef $guard;
            if (DEBUG) {
                debugf("Received response from %s => code = %s, message = %s", $backend->name, $_[1]->{Status}, $_[1]->{Reason});
            }
            delete $_[1]->{URL};
            delete $_[1]->{HTTPVersion};
            delete $_[1]->{Reason};
            $cv->send([
                delete $_[1]->{Status},
                [ %{$_[1]} ],
                [ $_[0] ]
            ]);
        }
    );
    return $cv;
}


no Moo;

1;

__END__

=head1 NAME

Kage - Perl Port of Kage

=head1 SYNOPSIS

    # app.psgi
    use strict;
    use Kage;

    my $server = Kage->new;
    $server->add_master(production => (
        host => "myapp.production.example.com",
        port => 80
    ));
    $server->add_backend(staging => (
        host => "myapp.staging.example.com",
        port => 8000
    ));
    $server->on(select_backend => sub {
        return [ "staging", "production" ]
    });
    $server->on(backend_finished => sub {
        my $responses = shift;
        my $data_production = $responses->{production}->{response}->decoded_content;
        my $data_staging    = $responses->{staging}->{response}->decoded_content;
        ...
    });

    $server->psgi_app;

    # run it
    twiggy -a app.psgi

=head1 DESCRIPTION

This is a port of kage to perl. Why does this exist? Because I felt like writing
it, duh.

=cut

