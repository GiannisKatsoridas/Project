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
        Actions.h
        JobQueue.c
        Jobs.c JobScheduler.c JobScheduler.h JoinEnumeration.c JoinEnumeration.h)
add_executable(DataGenerator
        DataGenerator.c)
add_executable(UnitTests
        UnitTests.c
        DataParse.c
        RadixHashJoin.c
        Results.c
        Tables.c
        Index.c
        Query.c
        Actions.c
        Jobs.c
        JobQueue.c
        JoinEnumeration.c
        JobScheduler.c
        CUnit/Sources/Basic/Basic.c
        CUnit/Sources/Framework/CUError.c
        CUnit/Sources/Framework/MyMem.c
        CUnit/Sources/Framework/TestDB.c
        CUnit/Sources/Framework/TestRun.c
        CUnit/Sources/Framework/Util.c)

target_link_libraries(Caramel pthread)
target_link_libraries(Caramel m)

target_link_libraries(UnitTests pthread)
target_link_libraries(UnitTests m)
