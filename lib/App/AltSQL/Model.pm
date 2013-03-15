package App::AltSQL::Model;

use Moose;

with 'App::AltSQL::Role';
with 'MooseX::Object::Pluggable';

has 'dbh'        => (is => 'rw');
has 'current_database' => (is => 'rw');

no Moose;
__PACKAGE__->meta->make_immutable();

sub show_sql_error {
	my ($self, $input, $char_number, $line_number) = @_;

	my @lines = split /\n/, $input;
	my $line = $lines[ $line_number - 1 ];
	$self->log_error("There was an error parsing the SQL statement on line $line_number:");
	$self->log_error($line);
	$self->log_error(('-' x ($char_number - 1)) . '^');
}

sub execute_sql {
	my ($self, $input, $recurse) = @_;
    
        if ($recurse) {
          warn "rerunning sql";
        }
        eval{
          my $sth = $self->dbh->prepare($input);
          $sth->execute();
        }; 
	if (my $error = $self->dbh->errstr || $@) {
         if ($error eq 'no connection to the server'
             ||
             $error =~ /terminating connection due to administrator command/
            ) {
             warn "!!no connection";
#             $self->log_error("!!".$error);
             eval {$self->db_connect();}; die $@ if $@;
             return $self->execute_sql($input, 1) if !$recurse;
         }else {
            $self->log_error($error);            
        }
		return;
	}

	return $sth;
}

1;
