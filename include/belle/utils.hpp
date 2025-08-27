#pragma once
#include <string>
#include <optional>
#include <vector>
#include <sstream>

// ssl support
#include <boost/asio/io_context.hpp>

#ifndef OB_BELLE_CONFIG_SSL_OFF
#define OB_BELLE_CONFIG_SSL_ON
#endif // OB_BELLE_CONFIG_SSL_OFF

#ifdef OB_BELLE_CONFIG_SSL_ON
#include <boost/beast/websocket/ssl.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/ssl/stream.hpp>
#endif // OB_BELLE_CONFIG_SSL_ON

namespace net = boost::asio;

namespace OB::Belle::Detail {

// string to lowercase
inline std::string lowercase(std::string str)
{
  auto const to_lower = [](char& c)
  {
    if (c >= 'A' && c <= 'Z')
    {
      c += 'a' - 'A';
    }

    return c;
  };

  for (char& c : str)
  {
    c = to_lower(c);
  }

  return str;
}

// find extension if present in a string path
inline std::optional<std::string> extension(std::string const& path)
{
  if (path.empty() || path.size() < 2)
  {
    return {};
  }

  auto const pos = path.rfind(".");

  if (pos == std::string::npos || pos == path.size() - 1)
  {
    return {};
  }

  return path.substr(pos + 1);
}

// split a string by a delimiter 'n' times
inline std::vector<std::string> split(std::string const& str, std::string const& delim, std::size_t times = 0)
{
  std::vector<std::string> vtok;
  std::size_t start {0};
  auto end = str.find(delim);

  if(times == 0){
    while (end != std::string::npos)
    {
        vtok.emplace_back(str.substr(start, end - start));
        start = end + delim.length();
        end = str.find(delim, start);
    }
  }else{
    while ((times-- > 0) && (end != std::string::npos))
    {
      vtok.emplace_back(str.substr(start, end - start));
      start = end + delim.length();
      end = str.find(delim, start);
    }
  }
  vtok.emplace_back(str.substr(start, end));

  return vtok;
}

// convert object into a string
template<typename T>
inline std::string to_string(T const& t)
{
  std::stringstream ss;
  ss << t;

  return ss.str();
}

#ifdef OB_BELLE_CONFIG_SSL_ON
template <typename Stream_T>
using ssl_stream = net::ssl::stream<Stream_T>;
#endif // OB_BELLE_CONFIG_SSL_ON

} // namespace Detail
