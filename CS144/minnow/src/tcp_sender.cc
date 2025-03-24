#include "tcp_sender.hh"
#include "debug.hh"
#include "tcp_config.hh"
#include <byte_stream.hh>
#include <cstdint>

using namespace std;

// This function is for testing only; don't add extra state to support it.
uint64_t TCPSender::sequence_numbers_in_flight() const
{
  int count = 0;
  for ( auto& seg : unacked_segments_ ) {
    count += seg.sequence_length();
  }
  return count;
}

// This function is for testing only; don't add extra state to support it.
uint64_t TCPSender::consecutive_retransmissions() const
{
  debug( "unimplemented consecutive_retransmissions() called" );
  return {};
}

void TCPSender::push( const TransmitFunction& transmit )
{
  while ( window_size_ > 0 && !closed_ ) {
    string data;
    TCPSenderMessage msg = make_empty_message();
    msg.SYN = ( sended_len == 0 );

    window_size_ = ( window_size_ > msg.SYN ) ? ( window_size_ - msg.SYN ) : 0;
    read( reader(), min( window_size_, TCPConfig::MAX_PAYLOAD_SIZE ), data );
    msg.payload = std::move( data );
    window_size_ -= msg.payload.size();

    msg.FIN = reader().is_finished();
    window_size_ = ( window_size_ > msg.FIN ) ? ( window_size_ - msg.FIN ) : 0;

    if ( msg.sequence_length() == 0 )
      break;

    transmit( msg );
    unacked_segments_.push_back( msg );

    sended_len += msg.sequence_length();

    if ( msg.FIN )
      closed_ = true;
  }
}

TCPSenderMessage TCPSender::make_empty_message() const
{
  TCPSenderMessage msg;
  msg.seqno = isn_ + sended_len;
  return msg;
}

void TCPSender::receive( const TCPReceiverMessage& msg )
{
  uint64_t next_seqno_ = ( isn_ + sended_len ).unwrap( isn_, sended_len );

  window_size_ = ( msg.window_size == 0 ) ? 1 : msg.window_size;
  if ( msg.ackno.has_value() ) {
    ackno_ = msg.ackno.value().unwrap( isn_, sended_len );
    if ( ackno_ > next_seqno_ ) {
      return;
    }

    for ( auto it = unacked_segments_.begin(); it != unacked_segments_.end(); ) {
      if ( it->seqno.unwrap( isn_, sended_len ) + it->sequence_length() <= ackno_ ) {
        it = unacked_segments_.erase( it );
      } else {
        it++;
      }
    }
  }
}

void TCPSender::tick( uint64_t ms_since_last_tick, const TransmitFunction& transmit )
{
  debug( "unimplemented tick() called" );
  (void)ms_since_last_tick;
  (void)transmit;
}
