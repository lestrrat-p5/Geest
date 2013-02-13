use inc::Module::Install;

name 'Kage';
all_from 'lib/Kage.pm';

requires 'AnyEvent';
requires 'AnyEvent::Socket';
requires 'AnyEvent::HTTP';
requires 'HTTP::Message::PSGI';
requires 'HTTP::Request';
requires 'HTTP::Response';
requires 'Log::Minimal';
requires 'Moo'; # We could do without it, if it's a problem...
requires 'Plack::Request';
requires 'Twiggy';

WriteAll;