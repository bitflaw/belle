#pragma once
#ifndef OB_BELLE_CONFIG_SERVER_OFF
#define OB_BELLE_CONFIG_SERVER_ON
#endif
#include <unordered_set>
#include <unordered_map>
#include <regex>
#include <thread>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/bind_executor.hpp>

#include "./mime_types.hpp"
#include "./request.hpp"
#include "./ordered_map.hpp"
#include "./websocket_session.hpp"


namespace OB::Belle {

#ifdef OB_BELLE_CONFIG_SSL_ON
namespace ssl = boost::asio::ssl;
#endif // OB_BELLE_CONFIG_SSL_ON

namespace websocket = boost::beast::websocket;
using tcp = boost::asio::ip::tcp;
using error_code = boost::system::error_code;

class Server
{
public:

  // NOTE Channel implementation is NOT thread safe
  class Channel
  {
  public:

    Channel()
    {
    }

    void join(Websocket_Session& socket_)
    {
      _sockets.insert(&socket_);
    }

    void leave(Websocket_Session& socket_)
    {
      _sockets.erase(&socket_);
    }

    void broadcast(std::string const&& str_) const
    {
      for(auto const e : _sockets)
      {
        e->send(std::move(str_));
      }
    }

    std::size_t size() const
    {
      return _sockets.size();
    }

  private:

    std::unordered_set<Websocket_Session*> _sockets;
  }; // class Channel

  // NOTE Channels implementation is NOT thread safe
  using Channels = std::unordered_map<std::string, Channel>;

  template<typename Body>
  struct Http_Ctx_Basic
  {
    Request req {};
    http::response<Body> res {};
    std::shared_ptr<void> data {nullptr};
  }; // class Http_Ctx_Basic

  using Http_Ctx = Http_Ctx_Basic<http::string_body>;

  class Websocket_Ctx
  {
  public:

    Websocket_Ctx(Websocket_Session& socket_, Request&& req_,
      Channels& channels_) :
      socket {&socket_},
      req {std::move(req_)},
      channels {channels_}
    {
    }

    ~Websocket_Ctx()
    {
    }

    void send(std::string const&& str_) const
    {
      socket->send(std::move(str_));
    }

    void broadcast(std::string const&& str_) const
    {
      for (auto const& e : channels)
      {
        e.second.broadcast(std::move(str_));
      }
    }

    Websocket_Session* socket;
    Request req;
    Channels& channels;
    std::string msg {};
    std::shared_ptr<void> data {nullptr};
  }; // class Websocket_Ctx

  // callbacks
  using fn_on_signal = std::function<void(error_code, int)>;
  using fn_on_http = std::function<void(Http_Ctx&)>;
  using fn_on_websocket = std::function<void(Websocket_Ctx&)>;

  struct fns_on_websocket
  {
    fns_on_websocket(fn_on_websocket const& begin_,
      fn_on_websocket const& data_, fn_on_websocket const& end_) :
      begin {begin_},
      data {data_},
      end {end_}
    {
    }

    fn_on_websocket begin {};
    fn_on_websocket data {};
    fn_on_websocket end {};
  }; // struct fns_on_websocket

  // aliases
  using Http_Routes =
    Ordered_Map<std::string, std::unordered_map<int, fn_on_http>>;

  using Websocket_Routes =
    std::vector<std::pair<std::string, fns_on_websocket>>;

private:

  struct Attr
  {
#ifdef OB_BELLE_CONFIG_SSL_ON
    // use ssl
    bool ssl {false};

    // ssl context
    ssl::context ssl_context {ssl::context::tlsv12_server};
#endif // OB_BELLE_CONFIG_SSL_ON

    // the public directory for serving static files
    std::string public_dir {};

    // default index filename for the public directory
    std::string index_file {"index.html"};

    // socket timeout
    std::chrono::seconds timeout {10};

    // serve static files from public directory
    bool http_static {true};

    // serve dynamic content
    bool http_dynamic {true};

    // upgrade http to websocket connection
    bool websocket {true};

    // default http headers
    Headers http_headers {};

    // http routes
    Http_Routes http_routes {};

    // websocket routes
    Websocket_Routes websocket_routes {};

    // callbacks for http
    fn_on_http on_http_error {};
    fn_on_http on_http_connect {};
    fn_on_http on_http_disconnect {};

    // callbacks for websocket
    fn_on_websocket on_websocket_error {};
    fn_on_websocket on_websocket_connect {};
    fn_on_websocket on_websocket_disconnect {};

    // websocket channels
    Channels channels {};
  }; // struct Attr

  template<typename Derived>
  class Websocket_Base : public Websocket_Session
  {
    Derived& derived()
    {
      return static_cast<Derived&>(*this);
    }

  public:

    Websocket_Base(net::io_context& io_, std::shared_ptr<Attr> const attr_,
      Request&& req_, fns_on_websocket const& on_websocket_) :
      _attr {attr_},
      _ctx {static_cast<Derived&>(*this), std::move(req_), _attr->channels},
      _on_websocket {on_websocket_},
      _strand {io_.get_executor()}
    {
    }

    ~Websocket_Base()
    {
      // leave channel
      _attr->channels.at(_ctx.req.path().at(0)).leave(derived());

      if (_on_websocket.end)
      {
        try
        {
          // run user function
          _on_websocket.end(_ctx);
        }
        catch (...)
        {
          this->handle_error();
        }
      }

      if (_attr->on_websocket_disconnect)
      {
        try
        {
          // run user function
          _attr->on_websocket_disconnect(_ctx);
        }
        catch (...)
        {
          this->handle_error();
        }
      }
    }

    void send(std::string const&& str_)
    {
      auto const pstr = std::make_shared<std::string const>(std::move(str_));
      _que.emplace_back(pstr);

      if (_que.size() > 1)
      {
        return;
      }

      derived().socket().async_write(net::buffer(*_que.front()),
        [self = derived().shared_from_this()](error_code ec, std::size_t bytes)
        {
          self->on_write(ec, bytes);
        }
      );
    }

    void handle_error()
    {
      if (_attr->on_websocket_error)
      {
        try
        {
          // run user function
          _attr->on_websocket_error(_ctx);
        }
        catch (...)
        {
        }
      }
    }

    void do_accept()
    {
      derived().socket().control_callback(
        [this](websocket::frame_type type, boost::beast::string_view data)
        {
          this->on_control_callback(type, data);
        }
      );

      derived().socket().set_option(
        beast::websocket::stream_base::decorator(
          [&](auto& res)
          {
            for (auto const& e : _attr->http_headers)
            {
              res.insert(e.name_string(), e.value());
            }
          }
        )
      );

      derived().socket().async_accept(_ctx.req,
        net::bind_executor(_strand,
          [self = derived().shared_from_this()](error_code ec)
          {
            self->on_accept(ec);
          }
        )
      );
    }

    void on_accept(error_code ec_)
    {
      if (ec_ == net::error::operation_aborted)
      {
        return;
      }

      if (ec_)
      {
        // TODO log here
        return;
      }

      // join channel
      if (_attr->channels.find(_ctx.req.path().at(0)) == _attr->channels.end())
      {
        _attr->channels[_ctx.req.path().at(0)] = Channel();
      }
      _attr->channels.at(_ctx.req.path().at(0)).join(derived());

      if (_attr->on_websocket_connect)
      {
        try
        {
          // run user function
          _attr->on_websocket_connect(_ctx);
        }
        catch (...)
        {
          this->handle_error();
        }
      }

      if (_on_websocket.begin)
      {
        try
        {
          // run user function
          _on_websocket.begin(_ctx);
        }
        catch (...)
        {
          this->handle_error();
        }
      }

      this->do_read();
    }

    void on_control_callback(websocket::frame_type type_, boost::beast::string_view data_)
    {
      boost::ignore_unused(type_, data_);
    }

    void do_read()
    {
      derived().socket().async_read(_buf,
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

      // socket closed by the timer
      if (ec_ == net::error::operation_aborted)
      {
        return;
      }

      // socket closed
      if (ec_ == websocket::error::closed)
      {
        return;
      }

      if (ec_)
      {
        // TODO log here
        return;
      }

      if (_on_websocket.data)
      {
        try
        {
          _ctx.msg = boost::beast::buffers_to_string(_buf.data());

          // run user function
          _on_websocket.data(_ctx);
        }
        catch (...)
        {
          handle_error();
        }
      }

      // clear the request object
      _ctx.req.clear();

      // clear the buffers
      _buf.consume(_buf.size());

      this->do_read();
    }

    void on_write(error_code ec_, std::size_t bytes_)
    {
      boost::ignore_unused(bytes_);

      // happens when the timer closes the socket
      if (ec_ == net::error::operation_aborted)
      {
        return;
      }

      if (ec_)
      {
        // TODO log here
        return;
      }

      // remove sent message from the queue
      _que.pop_front();

      if (_que.empty())
      {
        return;
      }

      derived().socket().async_write(net::buffer(*_que.front()),
        [self = derived().shared_from_this()](error_code ec, std::size_t bytes)
        {
          self->on_write(ec, bytes);
        }
      );
    }

    std::shared_ptr<Attr> const _attr;
    Websocket_Ctx _ctx;
    fns_on_websocket const& _on_websocket;
    net::strand<net::io_context::executor_type> _strand;
    boost::beast::multi_buffer _buf;
    std::deque<std::shared_ptr<std::string const>> _que {};
  }; // class Websocket_Base

  class Websocket :
    public Websocket_Base<Websocket>,
    public std::enable_shared_from_this<Websocket>
  {
  public:

    Websocket(tcp::socket&& socket_, std::shared_ptr<Attr> const attr_,
      Request&& req_, fns_on_websocket const& on_websocket_) :
      Websocket_Base {static_cast<net::io_context&>(socket_.get_executor().context()), attr_,
        std::move(req_), on_websocket_},
      _socket {std::move(socket_)}
    {
    }

    ~Websocket()
    {
    }

    websocket::stream<tcp::socket>& socket()
    {
      return _socket;
    }

    void run()
    {
      this->do_accept();
    }

    void do_timeout()
    {
      this->do_shutdown();
    }

    void do_shutdown()
    {
      _socket.async_close(websocket::close_code::normal,
        net::bind_executor(this->_strand,
          [self = this->shared_from_this()](error_code ec)
          {
            self->on_shutdown(ec);
          }
        )
      );
    }

    void on_shutdown(error_code ec_)
    {
      if (ec_)
      {
        // TODO log here
        return;
      }
    }

  private:

    websocket::stream<tcp::socket> _socket;
  }; // class Websocket

#ifdef OB_BELLE_CONFIG_SSL_ON
  class Websockets :
    public Websocket_Base<Websockets>,
    public std::enable_shared_from_this<Websockets>
  {
  public:

    Websockets(Detail::ssl_stream<tcp::socket>&& socket_, std::shared_ptr<Attr> const attr_,
      Request&& req_, fns_on_websocket const& on_websocket_) :
      Websocket_Base {static_cast<net::io_context&>(socket_.get_executor().context()), attr_,
        std::move(req_), on_websocket_},
      _socket {std::move(socket_)}
    {
    }

    ~Websockets()
    {
    }

    websocket::stream<Detail::ssl_stream<tcp::socket>>& socket()
    {
      return _socket;
    }

    void run()
    {
      this->do_accept();
    }

    void do_timeout()
    {
      this->do_shutdown();
    }

    void do_shutdown()
    {
      _socket.async_close(websocket::close_code::normal,
        net::bind_executor(this->_strand,
          [self = this->shared_from_this()](error_code ec)
          {
            self->on_shutdown(ec);
          }
        )
      );
    }

    void on_shutdown(error_code ec_)
    {
      if (ec_)
      {
        // TODO log here
        return;
      }
    }

  private:

    websocket::stream<Detail::ssl_stream<tcp::socket>> _socket;
  }; // class Websockets
#endif // OB_BELLE_CONFIG_SSL_ON

  template<typename Derived, typename Websocket_Type>
  class Http_Base
  {
    Derived& derived()
    {
      return static_cast<Derived&>(*this);
    }

  public:

    Http_Base(net::io_context& io_, std::shared_ptr<Attr> const attr_) :
      _strand {io_.get_executor()},
      _timer {io_, (std::chrono::steady_clock::time_point::max)()},
      _attr {attr_}
    {
    }

    ~Http_Base()
    {
    }

// TODO remove shim once visual studio supports generic lambdas
#ifdef _MSC_VER
    template<typename Self, typename Res>
    static void constexpr send(Self self, Res&& res)
#else
    // generic lambda for sending different types of responses
    static auto constexpr send = [](auto self, auto&& res) -> void
#endif // _MSC_VER
    {
      using item_type = std::remove_reference_t<decltype(res)>;

      auto ptr = std::make_shared<item_type>(std::move(res));
      self->_res = ptr;

      http::async_write(self->derived().socket(), *ptr,
        net::bind_executor(self->_strand,
          [self, close = ptr->need_eof()]
          (error_code ec, std::size_t bytes)
          {
            self->on_write(ec, bytes, close);
          }
        )
      );
    };

    int serve_static()
    {
      if (! _attr->http_static || _attr->public_dir.empty())
      {
        return 404;
      }

      if ((_ctx.req.method() != http::verb::get) && (_ctx.req.method() != http::verb::head))
      {
        return 404;
      }

      std::string half_path {_ctx.req.target()};
      std::string path {_attr->public_dir + half_path};

      if (path.back() == '/')
      {
        path += _attr->index_file;
      }

      error_code ec;
      http::file_body::value_type body;
      body.open(path.data(), beast::file_mode::scan, ec);

      if (ec)
      {
        return 404;
      }

      // head request
      if (_ctx.req.method() == http::verb::head)
      {
        http::response<http::empty_body> res {};
        res.base() = http::response_header<>(_attr->http_headers);
        res.version(_ctx.req.version());
        res.keep_alive(_ctx.req.keep_alive());
        res.content_length(body.size());
        res.set(Header::content_type, mime_type(path));
        send(derived().shared_from_this(), std::move(res));
        return 0;
      }

      // get request
      auto const size = body.size();
      http::response<http::file_body> res {
        std::piecewise_construct,
        std::make_tuple(std::move(body)),
        std::make_tuple(_attr->http_headers)
      };
      res.version(_ctx.req.version());
      res.keep_alive(_ctx.req.keep_alive());
      res.content_length(size);
      res.set(Header::content_type, mime_type(path));
      send(derived().shared_from_this(), std::move(res));
      return 0;
    }

    int serve_dynamic()
    {
      if (! _attr->http_dynamic || _attr->http_routes.empty())
      {
        return 404;
      }

      // regex variables
      std::smatch rx_match {};
      std::regex_constants::syntax_option_type const rx_opts {std::regex::ECMAScript};
      std::regex_constants::match_flag_type const rx_flgs {std::regex_constants::match_not_null};

      // the request path
      std::string path {_ctx.req.target()};

      // separate the query parameters
      auto params = Detail::split(path, "?", 1);
      path = params.at(0);

      // iterate over routes
      for (auto const& regex_method : _attr->http_routes)
      {
        bool method_match {false};
        auto match = (*regex_method).second.find(0);

        if (match != (*regex_method).second.end())
        {
          method_match = true;
        }
        else
        {
          match = (*regex_method).second.find(static_cast<int>(_ctx.req.method()));

          if (match != (*regex_method).second.end())
          {
            method_match = true;
          }
        }

        if (method_match)
        {
          std::regex rx_str {(*regex_method).first, rx_opts};
          if (std::regex_match(path, rx_match, rx_str, rx_flgs))
          {
            // set the path
            for (auto const& e : rx_match)
            {
              _ctx.req.path().emplace_back(e.str());
            }

            // parse target params
            _ctx.req.params_parse();

            // set callback function
            auto const& user_func = match->second;

            try
            {
              // run user function
              user_func(_ctx);

              _ctx.res.content_length(_ctx.res.body().size());
              send(derived().shared_from_this(), std::move(_ctx.res));
              return 0;
            }
            catch (int const e)
            {
              return e;
            }
            catch (unsigned int const e)
            {
              return static_cast<int>(e);
            }
            catch (Status const e)
            {
              return static_cast<int>(e);
            }
            catch (std::exception const&)
            {
              return 500;
            }
            catch (...)
            {
              return 500;
            }
          }
        }
      }

      return 404;
    }

    void serve_error(int err)
    {
      _ctx.res.result(static_cast<unsigned int>(err));

      if (_attr->on_http_error)
      {
        try
        {
          // run user function
          _attr->on_http_error(_ctx);

          _ctx.res.content_length(_ctx.res.body().size());
          send(derived().shared_from_this(), std::move(_ctx.res));
          return;
        }
        catch (int const e)
        {
          _ctx.res.result(static_cast<unsigned int>(e));
        }
        catch (unsigned int const e)
        {
          _ctx.res.result(e);
        }
        catch (Status const e)
        {
          _ctx.res.result(e);
        }
        catch (std::exception const&)
        {
          _ctx.res.result(500);
        }
        catch (...)
        {
          _ctx.res.result(500);
        }
      }

      _ctx.res.set(Header::content_type, "text/plain");
      _ctx.res.body() = "Error: " + std::to_string(_ctx.res.result_int());
      _ctx.res.content_length(_ctx.res.body().size());
      send(derived().shared_from_this(), std::move(_ctx.res));
    };

    void handle_request()
    {
      // set default response values
      _ctx.res.version(_ctx.req.version());
      _ctx.res.keep_alive(_ctx.req.keep_alive());

      if (_ctx.req.target().empty())
      {
        _ctx.req.target() = "/";
      }

      if (_ctx.req.target().at(0) != '/' ||
        _ctx.req.target().find("..") != boost::beast::string_view::npos)
      {
        this->serve_error(404);
        return;
      }

      // serve dynamic content
      auto dyna = this->serve_dynamic();
      // success
      if (dyna == 0)
      {
        return;
      }
      // error
      if (dyna != 404)
      {
        this->serve_error(dyna);
        return;
      }

      // serve static content
      auto stat = this->serve_static();
      if (stat != 0)
      {
        this->serve_error(stat);
        return;
      }
    }

    bool handle_websocket()
    {
      // the request path
      std::string path {_ctx.req.target()};

      // separate the query parameters
      auto params = Detail::split(path, "?", 1);
      path = params.at(0);

      // regex variables
      std::smatch rx_match {};
      std::regex_constants::syntax_option_type const rx_opts {std::regex::ECMAScript};
      std::regex_constants::match_flag_type const rx_flgs {std::regex_constants::match_not_null};

      // check for matching route
      for (auto const& [regex, callback] : _attr->websocket_routes)
      {
        std::regex rx_str {regex, rx_opts};

        if (std::regex_match(path, rx_match, rx_str, rx_flgs))
        {
          // set the path
          for (auto const& e : rx_match)
          {
            _ctx.req.path().emplace_back(e.str());
          }

          // parse target params
          _ctx.req.params_parse();

          // create websocket
          std::make_shared<Websocket_Type>
            (derived().socket_move(), _attr, std::move(_ctx.req), callback)
            ->run();

          return true;
        }
      }

      return false;
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

    void on_timer(error_code ec_ = {})
    {
      if (ec_ && ec_ != net::error::operation_aborted)
      {
        // TODO log here
        return;
      }

      // check if socket has been upgraded or closed
      if (_timer.expiry() == std::chrono::steady_clock::time_point::min())
      {
        return;
      }

      // check expiry
      if (_timer.expiry() <= std::chrono::steady_clock::now())
      {
        derived().do_timeout();
        return;
      }
    }

    void do_read()
    {
      _timer.expires_after(_attr->timeout);

      _res = nullptr;
      _ctx = {};
      _ctx.res.base() = http::response_header<>(_attr->http_headers);

      http::async_read(derived().socket(), _buf, _ctx.req,
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

      // the timer has closed the socket
      if (ec_ == net::error::operation_aborted)
      {
        return;
      }

      // the connection has been closed
      if (ec_ == http::error::end_of_stream)
      {
        derived().do_shutdown();
        return;
      }

      if (ec_)
      {
        // TODO log here
        return;
      }

      // check for websocket upgrade
      if (websocket::is_upgrade(_ctx.req))
      {
        if (! _attr->websocket || _attr->websocket_routes.empty())
        {
          derived().do_shutdown();
          return;
        }

        // upgrade to websocket
        if (handle_websocket())
        {
          this->cancel_timer();
          return;
        }
        else
        {
          derived().do_shutdown();
          return;
        }
      }

      if (_attr->on_http_connect)
      {
        try
        {
          // run user func
          _attr->on_http_connect(_ctx);
        }
        catch (...)
        {
        }
      }

      this->handle_request();

      if (_attr->on_http_disconnect)
      {
        try
        {
          // run user func
          _attr->on_http_disconnect(_ctx);
        }
        catch (...)
        {
        }
      }
    }

    void on_write(error_code ec_, std::size_t bytes_, bool close_)
    {
      boost::ignore_unused(bytes_);

      // the timer has closed the socket
      if (ec_ == net::error::operation_aborted)
      {
        return;
      }

      if (ec_)
      {
        // TODO log here
        return;
      }

      if (close_)
      {
        derived().do_shutdown();
        return;
      }

      // read another request
      this->do_read();
    }

    net::strand<net::io_context::executor_type> _strand;
    net::steady_timer _timer;
    boost::beast::flat_buffer _buf;
    std::shared_ptr<Attr> const _attr;
    Http_Ctx _ctx {};
    std::shared_ptr<void> _res {nullptr};
    bool _close {false};
  }; // class Http_Base

  class Http :
    public Http_Base<Http, Websocket>,
    public std::enable_shared_from_this<Http>
  {
  public:

    Http(tcp::socket socket_, std::shared_ptr<Attr> const attr_) :
      Http_Base {static_cast<net::io_context&>(socket_.get_executor().context()), attr_},
      _socket {std::move(socket_)}
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
      this->do_timer();
      this->do_read();
    }

    void do_timeout()
    {
      this->do_shutdown();
    }

    void do_shutdown()
    {
      error_code ec;

      // send a tcp shutdown
      _socket.shutdown(tcp::socket::shutdown_send, ec);

      this->cancel_timer();

      if (ec)
      {
        // TODO log here
        return;
      }
    }

  private:

    tcp::socket _socket;
  }; // class Http

#ifdef OB_BELLE_CONFIG_SSL_ON
  class Https :
    public Http_Base<Https, Websockets>,
    public std::enable_shared_from_this<Https>
  {
  public:

    Https(tcp::socket&& socket_, std::shared_ptr<Attr> const attr_) :
      Http_Base {static_cast<net::io_context&>(socket_.get_executor().context()), attr_},
      _socket {std::move(socket_), attr_->ssl_context}
    {
      this->_close = true;
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
      this->do_timer();
      this->do_handshake();
    }

    void do_timeout()
    {
      // timed out on handshake or shutdown
      if (this->_close)
      {
        return;
      }

      // reset the timer
      this->_timer.expires_at((std::chrono::steady_clock::time_point::max)());
      this->do_timer();

      this->do_shutdown();
    }

    void do_handshake()
    {
      this->_timer.expires_after(this->_attr->timeout);

      _socket.async_handshake(ssl::stream_base::server,
        net::bind_executor(this->_strand,
          [self = this->shared_from_this()](error_code ec)
          {
            self->on_handshake(ec);
          }
        )
      );
    }

    void on_handshake(error_code ec_)
    {
      // the timer has closed the socket
      if (ec_ == net::error::operation_aborted)
      {
        return;
      }

      if (ec_)
      {
        // TODO log here
        return;
      }

      this->_close = false;

      this->do_read();
    }

    void do_shutdown()
    {
      this->_timer.expires_after(this->_attr->timeout);

      this->_close = true;

      // shutdown the socket
      _socket.async_shutdown(
        net::bind_executor(this->_strand,
          [self = this->shared_from_this()](error_code ec)
          {
            self->on_shutdown(ec);
          }
        )
      );
    }

    void on_shutdown(error_code ec_)
    {
      this->cancel_timer();

      // the timer has closed the socket
      if (ec_ == net::error::operation_aborted)
      {
        return;
      }

      if (ec_)
      {
        // TODO log here
        return;
      }
    }

  private:

    Detail::ssl_stream<tcp::socket> _socket;
  }; // class Https
#endif // OB_BELLE_CONFIG_SSL_ON

  template<typename Session>
  class Listener : public std::enable_shared_from_this<Listener<Session>>
  {
  public:

    Listener(net::io_context& io_, tcp::endpoint endpoint_, std::shared_ptr<Attr> const attr_) :
      _acceptor {io_},
      _socket {io_},
      _attr {attr_}
    {
      error_code ec;

      // open the acceptor
      _acceptor.open(endpoint_.protocol(), ec);
      if (ec)
      {
        // TODO log here
        return;
      }

      // allow address reuse
      _acceptor.set_option(net::socket_base::reuse_address(true), ec);
      if (ec)
      {
        // TODO log here
        return;
      }

      // bind to the server address
      _acceptor.bind(endpoint_, ec);
      if (ec)
      {
        // TODO log here
        return;
      }

      // start listening for connections
      _acceptor.listen(net::socket_base::max_listen_connections, ec);

      if (ec)
      {
        // TODO log here
        return;
      }
    }

    void run()
    {
      if (! _acceptor.is_open())
      {
        // TODO log here
        return;
      }

      do_accept();
    }

  private:

    void do_accept()
    {
      _acceptor.async_accept(_socket,
        [self = this->shared_from_this()](error_code ec)
        {
          self->on_accept(ec);
        }
      );
    }

    void on_accept(error_code ec_)
    {
      if (ec_)
      {
        // TODO log here
      }
      else
      {
        // create an Http obj and run it
        std::make_shared<Session>(std::move(_socket), _attr)->run();
      }

      // accept another connection
      do_accept();
    }

  private:

    tcp::acceptor _acceptor;
    tcp::socket _socket;
    std::shared_ptr<Attr> const _attr;
  }; // class Listener

public:

  // default constructor
  Server()
  {
  }

  // constructor with address and port
  Server(std::string address_, unsigned short port_) :
    _address {address_},
    _port {port_}
  {
  }

#ifdef OB_BELLE_CONFIG_SSL_ON
  // constructor with address, port, and ssl
  Server(std::string address_, unsigned short port_, bool ssl_) :
    _address {address_},
    _port {port_}
  {
    _attr->ssl = true;
  }
#endif // OB_BELLE_CONFIG_SSL_ON

  // destructor
  ~Server()
  {
  }

  // set the listening address
  Server& address(std::string address_)
  {
    _address = address_;

    return *this;
  }

  // get the listening address
  std::string address()
  {
    return _address;
  }

  // set the listening port
  Server& port(unsigned short port_)
  {
    _port = port_;

    return *this;
  }

  // get the listening port
  unsigned short port()
  {
    return _port;
  }

  // set the public directory for serving static files
  Server& public_dir(std::string public_dir_)
  {
    if (! public_dir_.empty() && public_dir_.back() == '/')
    {
      public_dir_.pop_back();
    }

    if (public_dir_.empty())
    {
      public_dir_ = ".";
    }

    _attr->public_dir = public_dir_;

    return *this;
  }

  // get the public directory for serving static files
  std::string public_dir()
  {
    return _attr->public_dir;
  }

  // set the default index filename
  Server& index_file(std::string index_file_)
  {
    if (index_file_.empty())
    {
      _attr->index_file = "index.html";
    }
    else
    {
      _attr->index_file = index_file_;
    }

    return *this;
  }

  // get the default index filename
  std::string index_file()
  {
    return _attr->index_file;
  }

  // set the number of threads
  Server& threads(unsigned int threads_)
  {
    _threads = std::max<unsigned int>(1, threads_);

    return *this;
  }

  // get the number of threads
  unsigned int threads()
  {
    return _threads;
  }

#ifdef OB_BELLE_CONFIG_SSL_ON
  // set ssl
  Server& ssl(bool ssl_)
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
  Server& ssl_context(ssl::context&& ctx_)
  {
    _attr->ssl_context = std::move(ctx_);

    return *this;
  }
#endif // OB_BELLE_CONFIG_SSL_ON

  // set http static
  Server& http_static(bool val_)
  {
    _attr->http_static = val_;

    return *this;
  }

  // get http static
  bool http_static()
  {
    return _attr->http_static;
  }

  // set http dynamic
  Server& http_dynamic(bool val_)
  {
    _attr->http_dynamic = val_;

    return *this;
  }

  // get http dynamic
  bool http_dynamic()
  {
    return _attr->http_dynamic;
  }

  // set http static and dynamic
  Server& http(bool val_)
  {
    _attr->http_static = val_;
    _attr->http_dynamic = val_;

    return *this;
  }

  // set websocket upgrade
  Server& websocket(bool val_)
  {
    _attr->websocket = val_;

    return *this;
  }

  // get websocket upgrade
  bool websocket()
  {
    return _attr->websocket;
  }

  // set the socket timeout
  Server& timeout(std::chrono::seconds timeout_)
  {
    _attr->timeout = timeout_;

    return *this;
  }

  // get the socket timeout
  std::chrono::seconds timeout()
  {
    return _attr->timeout;
  }

  // get the io_context
  net::io_context& io()
  {
    return _io;
  }

  // set signals to capture
  Server& signals(std::vector<int> signals_)
  {
    for (auto const& e : signals_)
    {
      _signals.add(e);
    }

    return *this;
  }

  // set signal callback
  // called when a captured signal is received
  Server& on_signal(fn_on_signal on_signal_)
  {
    _on_signal = on_signal_;

    _signals.async_wait(
      [this](error_code const& ec, int sig)
      {
        this->_on_signal(ec, sig);
      }
    );

    return *this;
  }

  // set http callback matching a single method
  // called after http read
  Server& on_http(std::string route_, Method method_, fn_on_http on_http_)
  {
    if (_attr->http_routes.find(route_) == _attr->http_routes.map_end())
    {
      _attr->http_routes(route_, {{static_cast<int>(method_), on_http_}});
    }
    else
    {
      _attr->http_routes.at(route_)[static_cast<int>(method_)] = on_http_;
    }

    return *this;
  }

  // set http callback matching multiple methods
  // called after http read
  Server& on_http(std::string route_, std::vector<Method> methods_, fn_on_http on_http_)
  {
    for (auto const& e : methods_)
    {
      if (_attr->http_routes.find(route_) == _attr->http_routes.map_end())
      {
        _attr->http_routes(route_, {{static_cast<int>(e), on_http_}});
      }
      else
      {
        _attr->http_routes.at(route_)[static_cast<int>(e)] = on_http_;
      }
    }

    return *this;
  }

  // set http callback matching all methods
  // called after http read
  Server& on_http(std::string route_, fn_on_http on_http_)
  {
    if (_attr->http_routes.find(route_) == _attr->http_routes.map_end())
    {
      _attr->http_routes(route_, {{0, on_http_}});
    }
    else
    {
      _attr->http_routes.at(route_)[0] = on_http_;
    }

    return *this;
  }

  // set http error callback
  // called when an exception or error occurs
  Server& on_http_error(fn_on_http on_http_error_)
  {
    _attr->on_http_error = on_http_error_;

    return *this;
  }

  // set http connect callback
  // called at the very beginning of every http connection
  Server& on_http_connect(fn_on_http on_http_connect_)
  {
    _attr->on_http_connect = on_http_connect_;

    return *this;
  }

  // set http disconnect callback
  // called at the very end of every http connection
  Server& on_http_disconnect(fn_on_http on_http_disconnect_)
  {
    _attr->on_http_disconnect = on_http_disconnect_;

    return *this;
  }

  // set websocket data callback
  // data: called after every websocket read
  Server& on_websocket(std::string route_, fn_on_websocket data_)
  {
    _attr->websocket_routes.emplace_back(
      std::make_pair(route_, fns_on_websocket(nullptr, data_, nullptr)));

    return *this;
  }

  // set websocket begin, data, and end callbacks
  // begin: called once after connected
  // data: called after every websocket read
  // end: called once after disconnected
  Server& on_websocket(std::string route_,
    fn_on_websocket begin_, fn_on_websocket data_, fn_on_websocket end_)
  {
    _attr->websocket_routes.emplace_back(
      std::make_pair(route_, fns_on_websocket(begin_, data_, end_)));

    return *this;
  }

  // set websocket error callback
  // called when an exception or error occurs
  Server& on_websocket_error(fn_on_websocket on_websocket_error_)
  {
    _attr->on_websocket_error = on_websocket_error_;

    return *this;
  }

  // set websocket connect callback
  // called once at the very beginning after connected
  Server& on_websocket_connect(fn_on_websocket on_websocket_connect_)
  {
    _attr->on_websocket_connect = on_websocket_connect_;

    return *this;
  }

  // set websocket disconnect callback
  // called once at the very end after disconnected
  Server& on_websocket_disconnect(fn_on_websocket on_websocket_disconnect_)
  {
    _attr->on_websocket_disconnect = on_websocket_disconnect_;

    return *this;
  }

  // get http routes
  Http_Routes& http_routes()
  {
    return _attr->http_routes;
  }

  // get websocket routes
  Websocket_Routes& websocket_routes()
  {
    return _attr->websocket_routes;
  }

  // set default http headers
  Server& http_headers(Headers const& headers_)
  {
    _attr->http_headers = headers_;

    return *this;
  }

  // get default http headers
  Headers& http_headers()
  {
    return _attr->http_headers;
  }

  // get websocket channels
  Channels& channels()
  {
    return _attr->channels;
  }

  // check if address:port is already in use
  static bool available(std::string const& address_, unsigned short port_)
  {
    error_code ec;
    net::io_context io;
    tcp::acceptor acceptor(io);
    auto endpoint = tcp::endpoint(net::ip::make_address(address_), port_);

    acceptor.open(endpoint.protocol(), ec);

    if (ec)
    {
      return false;
    }

    acceptor.bind(endpoint, ec);

    if (ec)
    {
      return false;
    }

    return true;
  };

  // check if address:port is already in use
  bool available() const
  {
    error_code ec;
    net::io_context io;
    tcp::acceptor acceptor(io);
    auto endpoint = tcp::endpoint(net::ip::make_address(_address), _port);

    acceptor.open(endpoint.protocol(), ec);

    if (ec)
    {
      return false;
    }

    acceptor.bind(endpoint, ec);

    if (ec)
    {
      return false;
    }

    return true;
  };

  // start the server
  void listen(std::string address_ = "", unsigned short port_ = 0)
  {
    // set the listening address
    if (! address_.empty())
    {
      _address = address_;
    }

    // set the listening port
    if (port_ != 0)
    {
      _port = port_;
    }

    // set default server header value if not present
    if (_attr->http_headers.find(Header::server) == _attr->http_headers.end())
    {
      _attr->http_headers.set(Header::server, "Belle");
    }

    // websocket channels are not threadsafe, limit to 1 thread
    if (_attr->websocket && _threads > 1)
    {
      _threads = 1;
    }

    // create the listener
#ifdef OB_BELLE_CONFIG_SSL_ON
    if (_attr->ssl)
    {
      // use https
      std::make_shared<Listener<Https>>
        (_io, tcp::endpoint(net::ip::make_address(_address), _port), _attr)
        ->run();
    }
    else
#endif // OB_BELLE_CONFIG_SSL_ON
    {
      // use http
      std::make_shared<Listener<Http>>
        (_io, tcp::endpoint(net::ip::make_address(_address), _port), _attr)
        ->run();
    }

    // thread pool
    std::vector<std::thread> io_threads;

    // create and start threads if needed
    if (_threads > 1)
    {
      io_threads.reserve(static_cast<std::size_t>(_threads) - 1);

      for (unsigned int i = 1; i < _threads; ++i)
      {
        io_threads.emplace_back(
          [this]()
          {
            // run the io context on the new thread
            this->_io.run();
          }
        );
      }
    }

    // run the io context on the current thread
    _io.run();

    // wait on threads to return
    for (auto& t : io_threads)
    {
      t.join();
    }
  }

private:

  // hold the server attributes shared by each socket connection
  std::shared_ptr<Attr> const _attr {std::make_shared<Attr>()};

  // the address to listen on
  std::string _address {"127.0.0.1"};

  // the port to listen on
  unsigned short _port {8080};

  // the number of threads to run on
  unsigned int _threads {1};

  // the io context
  net::io_context _io {};

  // signals
  net::signal_set _signals {_io};

  // callback for signals
  fn_on_signal _on_signal {};
}; // class Server

}
