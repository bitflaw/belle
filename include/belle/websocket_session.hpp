#pragma once
#include <string>

namespace Belle {
// store a type erased websocket
struct Websocket_Session
{
  // default deconstructor
  virtual ~Websocket_Session() = default;

  // send a message
  virtual void send(std::string const&&) = 0;
}; // struct Websocket_Session

}
