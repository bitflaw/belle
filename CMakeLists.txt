cmake_minimum_required(VERSION 3.16 FATAL_ERROR)

project(belle VERSION 0.5.1 LANGUAGES CXX)

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

#generate version.hpp from version.hpp.in in include/
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/include/belle/version.hpp.in
  ${CMAKE_CURRENT_SOURCE_DIR}/include/belle/version.hpp
  @ONLY
)

#Install headers to system
install(DIRECTORY include/belle
  DESTINATION include
  FILES_MATCHING PATTERN "*.hpp"
)
