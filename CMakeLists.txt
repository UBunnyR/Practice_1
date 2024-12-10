cmake_minimum_required(VERSION 3.10)
project(my_cpp_project)

set(CMAKE_CXX_STANDARD 17)

# Добавляем путь к заголовочным файлам библиотеки
include_directories(${CMAKE_SOURCE_DIR}/../json/single_include)

add_executable(my_cpp_project main.cpp)