#include "tcp_receiver.hh"
#include "debug.hh"
#include "tcp_receiver_message.hh"
#include <cstdint>
using namespace std;

void TCPReceiver::receive( TCPSenderMessage message )
{
  // Your code here.
  if ( message.RST )
    reassembler_.set_error();
  if ( message.SYN )
    ISN_ = message.seqno;

  if ( ISN_.has_value() ) {
    uint64_t first_index
      = message.seqno.unwrap( ISN_.value(), reassembler_.first_unpoped_index() ) - ( !message.SYN );
    reassembler_.insert( first_index, message.payload, message.FIN );
    debug( "{}", reassembler_.count_bytes_pending() );
    next_seqno_ = Wrap32::wrap( reassembler_.first_unpoped_index(), ISN_.value() ) + 1
                  + ( reassembler_.writer().is_closed() );
  }
  send();
}

TCPReceiverMessage TCPReceiver::send() const
{
  // Your code here.
  TCPReceiverMessage message;
  message.window_size = get_window_size( reassembler_.writer().available_capacity() );
  if ( ISN_.has_value() )
    message.ackno = next_seqno_;
  if ( reassembler_.has_error() )
    message.RST = true;
  return message;
}
