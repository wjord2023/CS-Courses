#include "wrapping_integers.hh"
#include "debug.hh"
#include <cstdint>

using namespace std;

Wrap32 Wrap32::wrap( uint64_t n, Wrap32 zero_point )
{
  // Your code here.
  uint32_t wrap = n % ( 1LL << 32 );
  return zero_point + wrap;
}

uint64_t Wrap32::unwrap( Wrap32 zero_point, uint64_t checkpoint ) const
{
  // Your code here.
  uint64_t unwrap = raw_value_ - zero_point.raw_value_;
  if ( unwrap >= checkpoint )
    return unwrap;

  uint64_t bias = checkpoint - unwrap;
  uint64_t magnitude = bias / ( 1LL << 32 );
  uint64_t remainder = bias % ( 1LL << 32 );
  magnitude += ( remainder > ( 1LL << 31 ) );
  return unwrap + magnitude * ( 1LL << 32 );
}
