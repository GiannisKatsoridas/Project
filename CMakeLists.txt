cmake_minimum_required(VERSION 3.12)
project(Caramel C)

set(CMAKE_C_STANDARD 99)

add_executable(Caramel main.c Globals.h DataParse.c DataParse.h RadixHashJoin.c Results.c Results.h)
add_executable(DataGenerator DataGenerator.c DataGenerator.h)
