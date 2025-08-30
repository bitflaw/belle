// belle file upload example

//#include <belle/belle.hpp>
#include "../../../../include/belle/file_server.hpp"
#include <cassert>
#include <stdexcept>

#include <string>
#include <iostream>

int main(int argc, char *argv[])
{
  std::string upload_filename {};
  // init the server
  Belle::FileServer app {};

  // set the listening address
  std::string address {"127.0.0.1"};
  app.address(address);

  // set the listening port
  int port {8080};
  app.port(port);

  // enable serving static files from a public directory
  // if the path is relative, make sure to run the program
  // in the right working directory
  app.public_dir("../public");

  // handle route GET '/file'
  app.on_http("/file", Belle::Method::get, [&](Belle::FileServer::Http_Ctx& ctx)
  {

    ctx.req.params_parse();
    auto params = ctx.req.params();
    if (params.size() <= 0) {
      upload_filename = "../public/index.html";
    }else {
      auto it = params.find("filename");
      assert(it != params.end());
      upload_filename = it->second;
    }
    Belle::http::file_body::value_type file{};
    Belle::error_code ec;

    if(!std::filesystem::exists(upload_filename)){
      file.open("../public/index.html", Belle::beast::file_mode::scan, ec);
      if(!file.is_open()) throw std::runtime_error("ERROR: Failed to open file");
      ctx.res.body() = std::move(file);
      ctx.res.prepare_payload();
      throw std::runtime_error("file doesn't exist");
    }

    file.open(upload_filename.data(), Belle::beast::file_mode::scan, ec);
    if(!file.is_open()) throw std::runtime_error("ERROR: Failed to open file for response streaming!");

    // set http response headers
    ctx.res.set(Belle::Header::content_disposition, "attachment; filename=\""+upload_filename+"\"");
    ctx.res.set(Belle::Header::content_type, "application/octet-stream");
    ctx.res.prepare_payload();
    //set response body to file
    ctx.res.body() = std::move(file);
  });

  // print out the address and port
  // along with the routes
  std::cout
  << "Server: " << address << ":" << port << "\n\n"
  << "Try out the following urls:\n"
  << "  http://" << address << ":" << port << "/file\n"
  << "  http://" << address << ":" << port << "/index.html\n\n";

  // start the server
  app.listen();

  // the server blocks until a signal is received

  return 0;
}
