#pragma once

#ifndef OB_BELLE_CONFIG_CLIENT_OFF
#define OB_BELLE_CONFIG_CLIENT_ON
#endif

#include <deque>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/connect.hpp>

#include "./request.hpp"

namespace Belle {

#ifdef OB_BELLE_CONFIG_SSL_ON
namespace ssl = boost::asio::ssl;
#endif // OB_BELLE_CONFIG_SSL_ON

using tcp = boost::asio::ip::tcp;
using error_code = boost::system::error_code;

class Client
{
public:

  struct Http_Ctx
  {
    // http request
    Request* req {nullptr};

    // http response
    http::response<http::string_body> res {};
  }; // struct Http_Ctx

  struct Error_Ctx
  {
    // error code
    error_code const& ec;
  }; // struct Error_Ctx

  // callbacks
  using fn_on_http = std::function<void(Http_Ctx&)>;
  using fn_on_http_error = std::function<void(Error_Ctx&)>;

  struct Req_Ctx
  {
    // http request object
    Request req {};

    // http callback
    fn_on_http on_http {};
  }; // struct Req_Ctx

  struct Attr
  {
#ifdef OB_BELLE_CONFIG_SSL_ON
    // use ssl
    bool ssl {false};

    // ssl context
    ssl::context ssl_context {ssl::context::tlsv12_client};
#endif // OB_BELLE_CONFIG_SSL_ON

    // socket timeout
    std::chrono::seconds timeout {10};

    // address to connect to
    std::string address {"127.0.0.1"};

    // port to connect to
    unsigned short port {8080};

    // http request queue
    std::deque<Req_Ctx> que;

    // http error callback
    fn_on_http_error on_http_error {};
  }; // struct Attr

  template<typename Derived>
  class Http_Base
  {
    Derived& derived()
    {
      return static_cast<Derived&>(*this);
    }

  public:

    Http_Base(net::io_context& io_, std::shared_ptr<Attr> const attr_) :
      _resolver {io_},
      _strand {io_.get_executor()},
      _timer {io_, (std::chrono::steady_clock::time_point::max)()},
      _attr {attr_}
    {
    }

    ~Http_Base()
    {
    }

    void cancel_timer()
    {
      // set the timer to expire immediately
      _timer.expires_at((std::chrono::steady_clock::time_point::min)());
    }

    void do_timer()
    {
      // wait on the timer
      _timer.async_wait(
        net::bind_executor(_strand,
          [self = derived().shared_from_this()](error_code ec)
          {
            self->on_timer(ec);
          }
        )
      );
    }

    void on_timer(error_code ec_)
    {
      if (ec_ && ec_ != net::error::operation_aborted)
      {
        Error_Ctx err {ec_};
        _attr->on_http_error(err);
        return;
      }

      // check if socket has been closed
      if (_timer.expiry() == std::chrono::steady_clock::time_point::min())
      {
        return;
      }

      // check expiry
      if (_timer.expiry() <= std::chrono::steady_clock::now())
      {
        derived().do_close();

        return;
      }

      if (_close)
      {
        return;
      }
    }

    void do_resolve()
    {
      _timer.expires_after(_attr->timeout);

      // domain name server lookup
      _resolver.async_resolve(_attr->address,
        Detail::to_string(_attr->port),
        net::bind_executor(_strand,
          [self = derived().shared_from_this()]
          (error_code ec, tcp::resolver::results_type results)
          {
            self->on_resolve(ec, results);
          }
        )
      );
    }

    void on_resolve(error_code ec_, tcp::resolver::results_type results_)
    {
      if (ec_)
      {
        cancel_timer();
        Error_Ctx err {ec_};
        _attr->on_http_error(err);
        return;
      }

      // connect to the endpoint
      net::async_connect(derived().socket().lowest_layer(),
        results_.begin(), results_.end(),
        net::bind_executor(_strand,
          [self = derived().shared_from_this()](error_code ec, auto)
          {
            self->derived().on_connect(ec);
          }
        )
      );
    }

    void prepare_request()
    {
      _ctx = {};
      _ctx.req = &_attr->que.front().req;

      // serialize target and params
      _ctx.req->params_serialize();

      // set default user-agent header value if not present
      if (_ctx.req->find(Header::user_agent) == _ctx.req->end())
      {
        _ctx.req->set(Header::user_agent, "Belle");
      }

      // set default host header value if not present
      if (_ctx.req->find(Header::host) == _ctx.req->end())
      {
        _ctx.req->set(Header::host, _attr->address);
      }

      // set connection close if last request in the queue
      if (_attr->que.size() == 1)
      {
        _ctx.req->keep_alive(false);
      }

      // prepare the payload
      _ctx.req->prepare_payload();
    }

    void do_write()
    {
      prepare_request();

      _timer.expires_after(_attr->timeout);

      // Send the HTTP request
      http::async_write(derived().socket(), *_ctx.req,
        net::bind_executor(_strand,
          [self = derived().shared_from_this()](error_code ec, std::size_t bytes)
          {
            self->on_write(ec, bytes);
          }
        )
      );
    }

    void on_write(error_code ec_, std::size_t bytes_)
    {
      boost::ignore_unused(bytes_);

      if (ec_)
      {
        cancel_timer();
        Error_Ctx err {ec_};
        _attr->on_http_error(err);
        return;
      }

      do_read();
    }

    void do_read()
    {
      // Receive the HTTP response
      http::async_read(derived().socket(), _buf, _ctx.res,
        net::bind_executor(_strand,
          [self = derived().shared_from_this()](error_code ec, std::size_t bytes)
          {
            self->on_read(ec, bytes);
          }
        )
      );
    }

    void on_read(error_code ec_, std::size_t bytes_)
    {
      boost::ignore_unused(bytes_);

      if (ec_)
      {
        cancel_timer();
        Error_Ctx err {ec_};
        _attr->on_http_error(err);
        return;
      }

      // run user function
      _attr->que.front().on_http(_ctx);

      // remove request from queue
      _attr->que.pop_front();

      if (_attr->que.empty())
      {
        derived().do_close();
      }
      else
      {
        do_write();
      }
    }

    tcp::resolver _resolver;
    net::strand<net::io_context::executor_type> _strand;
    net::steady_timer _timer;
    std::shared_ptr<Attr> const _attr;
    Http_Ctx _ctx {};
    beast::flat_buffer _buf {};
    bool _close {false};
  }; // class Http_Base

  class Http :
    public Http_Base<Http>,
    public std::enable_shared_from_this<Http>
  {
  public:
    Http(net::io_context& io_, std::shared_ptr<Attr> attr_) :
      Http_Base<Http>(io_, attr_),
      _socket {io_}
    {
    }

    ~Http()
    {
    }

    tcp::socket& socket()
    {
      return _socket;
    }

    tcp::socket&& socket_move()
    {
      return std::move(_socket);
    }

    void run()
    {
      do_timer();
      do_resolve();
    }

    void on_connect(error_code ec_)
    {
      if (ec_)
      {
        cancel_timer();
        Error_Ctx err {ec_};
        _attr->on_http_error(err);
        return;
      }

      do_write();
    }

    void do_close()
    {
      error_code ec;

      // shutdown the socket
      _socket.shutdown(tcp::socket::shutdown_both, ec);
      _socket.close(ec);

      // ignore not_connected error
      if (ec && ec != boost::system::errc::not_connected)
      {
        cancel_timer();
        Error_Ctx err {ec};
        _attr->on_http_error(err);
        return;
      }

      // the connection is now closed
    }


  private:

    tcp::socket _socket;
  }; // class Http

#ifdef OB_BELLE_CONFIG_SSL_ON
  class Https :
    public Http_Base<Https>,
    public std::enable_shared_from_this<Https>
  {
  public:
    Https(net::io_context& io_, std::shared_ptr<Attr> attr_) :
      Http_Base<Https>(io_, attr_),
      _socket {tcp::socket(io_), attr_->ssl_context}
    {
      _close = true;
    }

    ~Https()
    {
    }

    Detail::ssl_stream<tcp::socket>& socket()
    {
      return _socket;
    }

    Detail::ssl_stream<tcp::socket>&& socket_move()
    {
      return std::move(_socket);
    }

    void run()
    {
      // start the timer
      do_timer();

      // set server name indication
      // use SSL_ctrl instead of SSL_set_tlsext_host_name macro
      // to avoid old style C cast to char*
      // if (! SSL_set_tlsext_host_name(_socket.native_handle(), _attr->address.data()))
      if (! SSL_ctrl(_socket.native_handle(), SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name, _attr->address.data()))
      {
        error_code ec
        {
          static_cast<int>(ERR_get_error()),
          net::error::get_ssl_category()
        };

        cancel_timer();
        Error_Ctx err {ec};
        _attr->on_http_error(err);
        return;
      }

      do_resolve();
    }

    void on_connect(error_code ec_)
    {
      if (ec_)
      {
        cancel_timer();
        Error_Ctx err {ec_};
        _attr->on_http_error(err);
        return;
      }

      do_handshake();
    }

    void do_handshake()
    {
      // perform the ssl handshake
      _socket.async_handshake(ssl::stream_base::client,
        net::bind_executor(_strand,
          [self = this->shared_from_this()](error_code ec)
          {
            self->on_handshake(ec);
          }
        )
      );
    }

    void on_handshake(error_code ec_)
    {
      if (ec_)
      {
        cancel_timer();
        Error_Ctx err {ec_};
        _attr->on_http_error(err);
        return;
      }

      _close = false;

      do_write();
    }

    void do_close()
    {
      if (_close)
      {
        return;
      }

      _close = true;

      // shutdown the socket
      _socket.async_shutdown(
        net::bind_executor(_strand,
          [self = this->shared_from_this()](error_code ec)
          {
            self->on_shutdown(ec);
          }
        )
      );
    }

    void on_shutdown(error_code ec_)
    {
      cancel_timer();

      // ignore eof error
      if (ec_ == net::error::eof)
      {
        ec_.assign(0, ec_.category());
      }

      // ignore not_connected error
      if (ec_ && ec_ != boost::system::errc::not_connected)
      {
        return;
      }

      // close the socket
      _socket.next_layer().close(ec_);

      // ignore not_connected error
      if (ec_ && ec_ != boost::system::errc::not_connected)
      {
        return;
      }

      // the connection is now closed
    }

  private:

    Detail::ssl_stream<tcp::socket> _socket;
  }; // class Https
#endif // OB_BELLE_CONFIG_SSL_ON

  // default constructor
  Client()
  {
  }

  // constructor with address and port
  Client(std::string address_, unsigned short port_)
  {
    _attr->address = address_;
    _attr->port = port_;
  }

#ifdef OB_BELLE_CONFIG_SSL_ON
  // constructor with address, port, and ssl
  Client(std::string address_, unsigned short port_, bool ssl_)
  {
    _attr->address = address_;
    _attr->port = port_;
    _attr->ssl = ssl_;
  }
#endif // OB_BELLE_CONFIG_SSL_ON

  // destructor
  ~Client()
  {
  }

  // set the address to connect to
  Client& address(std::string address_)
  {
    _attr->address = address_;

    return *this;
  }

  // get the address to connect to
  std::string address()
  {
    return _attr->address;
  }

  // set the port to connect to
  Client& port(unsigned short port_)
  {
    _attr->port = port_;

    return *this;
  }

  // get the port to connect to
  unsigned short port()
  {
    return _attr->port;
  }

  // set the socket timeout
  Client& timeout(std::chrono::seconds timeout_)
  {
    _attr->timeout = timeout_;

    return *this;
  }

  // get the socket timeout
  std::chrono::seconds timeout()
  {
    return _attr->timeout;
  }

  // set the max timeout
  Client& timeout_max(std::chrono::milliseconds timeout_max_)
  {
    _timeout_max = timeout_max_;

    return *this;
  }

  // get the max timeout
  std::chrono::milliseconds timeout_max()
  {
    return _timeout_max;
  }

  // get request queue
  std::deque<Req_Ctx>& queue()
  {
    return _attr->que;
  }

  // get the io_context
  net::io_context& io()
  {
    return _io;
  }

#ifdef OB_BELLE_CONFIG_SSL_ON
  // set ssl
  Client& ssl(bool ssl_)
  {
    _attr->ssl = ssl_;

    return *this;
  }

  // get ssl
  bool ssl()
  {
    return _attr->ssl;
  }

  // get the ssl context
  ssl::context& ssl_context()
  {
    return _attr->ssl_context;
  }

  // set the ssl context
  Client& ssl_context(ssl::context&& ctx_)
  {
    _attr->ssl_context = std::move(ctx_);

    return *this;
  }
#endif // OB_BELLE_CONFIG_SSL_ON

  Client& on_http(Request const& req_, fn_on_http on_http_)
  {
    _attr->que.emplace_back(Req_Ctx());
    auto& ctx = _attr->que.back();

    ctx.req = req_;
    ctx.on_http = on_http_;

    return *this;
  }

  Client& on_http(Request&& req_, fn_on_http on_http_)
  {
    _attr->que.emplace_back(Req_Ctx());
    auto& ctx = _attr->que.back();

    ctx.req = std::move(req_);
    ctx.on_http = on_http_;

    return *this;
  }

  Client& on_http(std::string const& target_, fn_on_http on_http_)
  {
    this->on_http_impl(Method::get, target_, Request::Params(), Headers(), {}, on_http_);

    return *this;
  }

  Client& on_http(std::string const& target_, Request::Params const& params_, fn_on_http on_http_)
  {
    this->on_http_impl(Method::get, target_, params_, Headers(), {}, on_http_);

    return *this;
  }

  Client& on_http(std::string const& target_, Headers const& headers_, fn_on_http on_http_)
  {
    this->on_http_impl(Method::get, target_, Request::Params(), headers_, {}, on_http_);

    return *this;
  }

  Client& on_http(std::string const& target_, Request::Params const& params_, Headers const& headers_, fn_on_http on_http_)
  {
    this->on_http_impl(Method::get, target_, params_, headers_, {}, on_http_);

    return *this;
  }

  Client& on_http(Method method_, std::string const& target_,
    std::string const& body_, fn_on_http on_http_)
  {
    this->on_http_impl(method_, target_, Request::Params(), Headers(), body_, on_http_);

    return *this;
  }

  Client& on_http(Method method_, std::string const& target_,
    Request::Params const& params_,
    std::string const& body_, fn_on_http on_http_)
  {
    this->on_http_impl(method_, target_, params_, Headers(), body_, on_http_);

    return *this;
  }

  Client& on_http(Method method_, std::string const& target_,
    Headers const& headers_,
    std::string const& body_, fn_on_http on_http_)
  {
    this->on_http_impl(method_, target_, Request::Params(), headers_, body_, on_http_);

    return *this;
  }

  Client& on_http(Method method_, std::string const& target_,
    Request::Params const& params_, Headers const& headers_,
    std::string const& body_, fn_on_http on_http_)
  {
    this->on_http_impl(method_, target_, params_, headers_, body_, on_http_);

    return *this;
  }

  Client& on_http_error(fn_on_http_error on_http_error_)
  {
    _attr->on_http_error = on_http_error_;

    return *this;
  }

  std::size_t connect()
  {
    if (_attr->que.empty())
    {
      return 0;
    }

#ifdef OB_BELLE_CONFIG_SSL_ON
    if (_attr->ssl)
    {
      // use https
      std::make_shared<Https>(_io, _attr)->run();
    }
    else
#endif // OB_BELLE_CONFIG_SSL_ON
    {
      // use http
      std::make_shared<Http>(_io, _attr)->run();
    }

    std::size_t size_begin {_attr->que.size()};

    if (_timeout_max > std::chrono::milliseconds(0))
    {
      // run for max 'n' amount of time
      _io.run_until(std::chrono::steady_clock::now() + _timeout_max);
    }
    else
    {
      _io.run();
    }

    // reset the io_context
    _io.restart();

    std::size_t size_end {_attr->que.size()};

    return size_begin - size_end;
  }

private:

  Client& on_http_impl(Method method_, std::string const& target_,
    Request::Params const& params_, Headers const& headers_,
    std::string const& body_, fn_on_http on_http_)
  {
    _attr->que.emplace_back(Req_Ctx());
    auto& ctx = _attr->que.back();

    Request req {method_, target_, 11, body_, headers_};
    req.params() = params_;

    ctx.req = std::move(req);
    ctx.on_http = on_http_;

    return *this;
  }

  // hold the client attributes
  std::shared_ptr<Attr> const _attr {std::make_shared<Attr>()};

  // the io context
  net::io_context _io {};

  // timeout all requests after specified number of milliseconds
  std::chrono::milliseconds _timeout_max {0};
}; // class Client

}
