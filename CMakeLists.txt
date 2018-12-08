cmake_minimum_required(VERSION 3.12)
project(Caramel C)

set(CMAKE_C_STANDARD 99)

add_executable(Caramel
        main.c
        DataParse.c
        RadixHashJoin.c
        Results.c
        Tables.c
        Index.c
        Query.c
        Actions.c
        Actions.h)
add_executable(DataGenerator
        DataGenerator.c)
add_executable(UnitTests
        UnitTests.c
        DataParse.c
        Index.c
        Results.c
        CUnit/Sources/Basic/Basic.c
        CUnit/Sources/Framework/CUError.c
        CUnit/Sources/Framework/MyMem.c
        CUnit/Sources/Framework/TestDB.c
        CUnit/Sources/Framework/TestRun.c
        CUnit/Sources/Framework/Util.c)