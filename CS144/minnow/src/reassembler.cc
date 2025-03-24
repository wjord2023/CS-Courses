#include "reassembler.hh"
#include "debug.hh"
#include <cstdint>

using namespace std;

void SubstringMap::insert_and_merge( uint64_t first_index, std::string data, bool is_last_substring )
{
  string new_data = data;
  uint64_t new_first_index = first_index;
  bool new_is_last_substring = is_last_substring;

  uint64_t last_index = first_index + data.size();

  auto it = this->begin();
  while ( it != this->end() ) {
    uint64_t start = it->first;
    uint64_t end = start + it->second.data.size();
    string& old_data = it->second.data;
    bool is_last = it->second.is_last_substring;

    if ( first_index < start && last_index > end ) {
      it = this->erase( it );
    } else if ( first_index >= start && last_index <= end ) {
      new_data = old_data.substr( 0, first_index - start ) + new_data + old_data.substr( last_index - start );
      new_first_index = start;
      new_is_last_substring = is_last;
      it = this->erase( it );
    } else if ( first_index <= start && last_index <= end && last_index >= start ) {
      new_data = new_data + old_data.substr( last_index - start );
      new_is_last_substring = is_last;
      it = this->erase( it );
    } else if ( first_index >= start && last_index >= end && first_index <= end ) {
      new_data = old_data.substr( 0, first_index - start ) + new_data;
      new_first_index = start;
      it = this->erase( it );
    } else {
      it++;
    }
  }

  this->insert( { new_first_index, Substring( new_data, new_is_last_substring ) } );
}

void Reassembler::insert( uint64_t first_index, string data, bool is_last_substring )
{
  if ( output_.writer().available_capacity() == 0 )
    return;

  uint64_t last_index = first_index + data.size();
  if ( first_index >= first_unacceptabled_index() )
    return;
  if ( last_index < first_unpoped_index_ )
    return;

  if ( first_index < first_unpoped_index_ ) {
    data = data.substr( first_unpoped_index_ - first_index );
    first_index = first_unpoped_index_;
  }
  if ( last_index > first_unacceptabled_index() ) {
    data = data.substr( 0, first_unacceptabled_index() - first_index );
    is_last_substring = false;
  }

  substrings_map.insert_and_merge( first_index, data, is_last_substring );

  while ( substrings_map.find( first_unpoped_index_ ) != substrings_map.end() ) {
    Substring& substring = substrings_map[first_unpoped_index_];
    uint64_t data_size = substring.data.size();
    bool is_last = substring.is_last_substring;

    output_.writer().push( substring.data );
    substrings_map.erase( first_unpoped_index_ );
    first_unpoped_index_ += data_size;
    if ( is_last ) {
      output_.writer().close();
      break;
    }
  }
}

// How many bytes are stored in the Reassembler itself?
// This function is for testing only; don't add extra state to support it.
uint64_t Reassembler::count_bytes_pending() const
{
  // debug( "unimplemented count_bytes_pending() called" );
  uint64_t bytes_pending = 0;
  for ( auto& [index, substring] : substrings_map ) {
    debug( "index: {}, data: {}", index, substring.data );
    bytes_pending += substring.data.size();
  }
  return bytes_pending;
}
