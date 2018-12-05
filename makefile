CC = gcc
FLAGS = -g

OBJ1 = DataParse.o RadixHashJoin.o main.o Results.o Index.o Tables.o Query.o Actions.o
HEAD1 = DataParse.h Results.h Globals.h Index.h Tables.h Query.h Actions.h
OUT1 = Caramel

OBJ2 = DataGenerator.o
HEAD2 = DataGenerator.h Globals.h
OUT2  = DataGenerator

BASIC_PATH = CUnit/Sources/Basic
FRAM_PATH = CUnit/Sources/Framework

OBJ3 = UnitTests.o DataParse.o Index.o Results.o $(BASIC_PATH)/Basic.o $(FRAM_PATH)/CUError.o $(FRAM_PATH)/MyMem.o $(FRAM_PATH)/TestDB.o $(FRAM_PATH)/TestRun.o $(FRAM_PATH)/Util.o
SOURCE3 = UnitTests.c DataParse.c Index.c results.c $(BASIC_PATH)/Basic.c $(FRAM_PATH)/CUError.c $(FRAM_PATH)/MyMem.c $(FRAM_PATH)/TestDB.c $(FRAM_PATH)/TestRun.c $(FRAM_PATH)/Util.c
HEAD3 = DataParse.h Index.h Results.h
OUT3 = UnitTests

TXT = DataRelationR.txt DataRelationS.txt


all: $(OUT1) $(OUT2) #$(OUT3)


Caramel: $(OBJ1) $(HEAD1)
	$(CC) $(OBJ1) -o $(OUT1)

DataGenerator: $(OBJ2) $(HEAD2)
	$(CC) $(OBJ2) -o $(OUT2)

UnitTests: $(OBJ3) $(HEAD3)
	$(CC) $(OBJ3) -o $(OUT3)

DataParse.o: DataParse.c DataParse.h Globals.h
	$(CC) -c DataParse.c $(FLAGS)

RadixHashJoin.o: RadixHashJoin.c Results.h Globals.h
	$(CC) -c RadixHashJoin.c $(FLAGS)

main.o: main.c DataParse.h Globals.h Tables.h Results.h Query.h Actions.h
	$(CC) -c main.c $(FLAGS)

Results.o: Results.c Results.h Globals.h
	$(CC) -c Results.c $(FLAGS)

Index.o: Index.c Index.h Results.h Globals.h
	$(CC) -c Index.c $(FLAGS)

Tables.o: Tables.c Tables.h Globals.h
	$(CC) -c Tables.c $(FLAGS)

Query.o: Query.c Query.h
	$(CC) -c Query.c $(FLAGS)

Actions.o: Actions.c Actions.h
	$(CC) -c Actions.c $(FLAGS)

DataGenerator.o: DataGenerator.c DataGenerator.h Globals.h
	$(CC) -c DataGenerator.c $(FLAGS)

UnitTests.o: UnitTests.c
	$(CC) -c UnitTests.c $(FLAGS)



clean:
	rm -f $(OUT1) $(OUT2) $(OUT3) 
	rm -f $(OBJ1) 
	rm -f $(OBJ2) 
#	rm -f $(OBJ3)

