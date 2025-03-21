#pragma once

#include "byte_stream.hh"
#include <cstdint>
#include <map>

struct Substring
{
  std::string data;
  bool is_last_substring;

  Substring() : data( "" ), is_last_substring( false ) {}
  Substring( const std::string& d, bool is_last ) : data( d ), is_last_substring( is_last ) {}

  uint64_t size() const { return data.size(); }
};

class SubstringMap : public std::map<uint64_t, Substring>
{
public:
  void insert_and_merge( uint64_t first_index, std::string data, bool is_last_substring );
};

class Reassembler
{
public:
  // Construct Reassembler to write into given ByteStream.
  explicit Reassembler( ByteStream&& output ) : output_( std::move( output ) ) {}

  uint64_t capacity() const { return output_.capacity(); }
  uint64_t first_unpoped_index() const { return first_unpoped_index_; }
  uint64_t first_unacceptabled_index() const { return first_unpoped_index_ + output_.capacity(); }

  void set_error() { output_.set_error(); }
  bool has_error() const { return output_.has_error(); }

  /*
   * Insert a new substring to be reassembled into a ByteStream.
   *   `first_index`: the index of the first byte of the substring
   *   `data`: the substring itself
   *   `is_last_substring`: this substring represents the end of the stream
   *   `output`: a mutable reference to the Writer
   *
   * The Reassembler's job is to reassemble the indexed substrings (possibly out-of-order
   * and possibly overlapping) back into the original ByteStream. As soon as the Reassembler
   * learns the next byte in the stream, it should write it to the output.
   *
   * If the Reassembler learns about bytes that fit within the stream's available capacity
   * but can't yet be written (because earlier bytes remain unknown), it should store them
   * internally until the gaps are filled in.
   *
   * The Reassembler should discard any bytes that lie beyond the stream's available capacity
   * (i.e., bytes that couldn't be written even if earlier gaps get filled in).
   *
   * The Reassembler should close the stream after writing the last byte.
   */
  void insert( uint64_t first_index, std::string data, bool is_last_substring );

  // How many bytes are stored in the Reassembler itself?
  // This function is for testing only; don't add extra state to support it.
  uint64_t count_bytes_pending() const;

  // Access output stream reader
  Reader& reader() { return output_.reader(); }
  const Reader& reader() const { return output_.reader(); }

  // Access output stream writer, but const-only (can't write from outside)
  const Writer& writer() const { return output_.writer(); }

private:
  ByteStream output_;
  uint64_t first_unpoped_index_ {};
  SubstringMap substrings_map {};
};
