#include "byte_stream.hh"
#include <string_view>

using namespace std;

ByteStream::ByteStream( uint64_t capacity ) : capacity_( capacity ) {}

void Writer::push( string data )
{
  if ( wt_closed_ )
    return;

  if ( data.size() > available_capacity() )
    data.resize( available_capacity() );

  for ( char b : data )
    buffer_.push( b );

  bytes_pushed_ += data.size();
}

void Writer::close()
{
  wt_closed_ = true;
}

bool Writer::is_closed() const
{
  return wt_closed_;
}

uint64_t Writer::available_capacity() const
{
  return capacity_ - buffer_.size();
}

uint64_t Writer::bytes_pushed() const
{
  return bytes_pushed_;
}

string_view Reader::peek() const
{
  if ( buffer_.empty() )
    return {};

  return { &buffer_.front(), 1 };
}

void Reader::pop( uint64_t len )
{
  if ( len > buffer_.size() )
    len = buffer_.size();

  for ( uint64_t i = 0; i < len; ++i )
    buffer_.pop();

  bytes_popped_ += len;
}

bool Reader::is_finished() const
{
  return wt_closed_ && buffer_.empty();
}

uint64_t Reader::bytes_buffered() const
{
  return buffer_.size();
}

uint64_t Reader::bytes_popped() const
{
  return bytes_popped_;
}
