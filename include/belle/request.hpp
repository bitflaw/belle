#pragma once
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include "./utils.hpp"

namespace Belle {

namespace beast = boost::beast;
namespace http = boost::beast::http;

using Method = beast::http::verb;
using Status = beast::http::status;
using Header = beast::http::field;
using Headers = beast::http::fields;


class Request : public http::request<http::string_body>
{
  using Base = http::request<http::string_body>;

public:

  using Path = std::vector<std::string>;
  using Params = std::unordered_multimap<std::string, std::string>;

  // inherit base constructors
  using http::request<http::string_body>::message;

  // default constructor
  Request() = default;

  // copy constructor
  Request(Request const&) = default;

  // move constructor
  Request(Request&&) = default;

  // copy assignment
  Request& operator=(Request const&) = default;

  // move assignment
  Request& operator=(Request&& rhs) = default;

  // default deconstructor
  ~Request() = default;

  Request&& move() noexcept
  {
    return std::move(*this);
  }

  // get the path
  Path& path()
  {
    return _path;
  }

  // get the query parameters
  Params& params()
  {
    return _params;
  }

  // serialize path and query parameters to the target
  void params_serialize()
  {
    std::string path {target()};

    _path.clear();
    _path.emplace_back(path);

    if (! _params.empty())
    {
      path += "?";
      auto it = _params.begin();
      for (; it != _params.end(); ++it)
      {
        path += url_encode(it->first) + "=" + url_encode(it->second) + "&";
      }
      path.pop_back();
    }

    target(path);
  }

  // parse the query parameters from the target
  void params_parse()
  {
    std::string path {target()};

    // separate the query params
    auto params = Detail::split(path, "?", 1);

    // set params
    if (params.size() == 2)
    {
      auto kv = Detail::split(params.at(1), "&");

      for (auto const& e : kv)
      {
        if (e.empty())
        {
          continue;
        }

        auto k_v = Detail::split(e, "=", 1);

        if (k_v.size() == 1)
        {
          _params.emplace(url_decode(e), "");
        }

        else if (k_v.size() == 2)
        {
          _params.emplace(url_decode(k_v.at(0)), url_decode(k_v.at(1)));
        }

        continue;
      }
    }
  }

private:

  std::string hex_encode(char const c)
  {
    char s[3];

    if (c & 0x80)
    {
      std::snprintf(&s[0], 3, "%02X",
        static_cast<unsigned int>(c & 0xff)
      );
    }
    else
    {
      std::snprintf(&s[0], 3, "%02X",
        static_cast<unsigned int>(c)
      );
    }

    return std::string(s);
  }

  char hex_decode(std::string const& s)
  {
    unsigned int n;

    std::sscanf(s.data(), "%x", &n);

    return static_cast<char>(n);
  }

  std::string url_encode(std::string const& str)
  {
    std::string res;
    res.reserve(str.size());

    for (auto const& e : str)
    {
      if (e == ' ')
      {
        res += "+";
      }
      else if (std::isalnum(static_cast<unsigned char>(e)) ||
        e == '-' || e == '_' || e == '.' || e == '~')
      {
        res += e;
      }
      else
      {
        res += "%" + hex_encode(e);
      }
    }

    return res;
  }

  std::string url_decode(std::string const& str)
  {
    std::string res;
    res.reserve(str.size());

    for (std::size_t i = 0; i < str.size(); ++i)
    {
      if (str[i] == '+')
      {
        res += " ";
      }
      else if (str[i] == '%' && i + 2 < str.size() &&
        std::isxdigit(static_cast<unsigned char>(str[i + 1])) &&
        std::isxdigit(static_cast<unsigned char>(str[i + 2])))
      {
        res += hex_decode(str.substr(i + 1, 2));
        i += 2;
      }
      else
      {
        res += str[i];
      }
    }

    return res;
  }

  Path _path {};
  Params _params {};
}; // Request

}
