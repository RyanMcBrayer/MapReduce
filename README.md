# MapReduce
A multi-threaded implementation of the MapReduce programming model, designed using Linux libraries and C.

# Background
This project was done as an assignment for my Operating Systems course. The header files, the example main.c that implements a word count program, and a single-threaded version of hashmap.c were provided. I was responsilbe for making a multi-threaded version of a MapReduce library in mapreduce.c, as well as modifying the provided hashmap.c to work with multithreading. The user can use the provided library specified in mapreduce.c to create their own main.c program, that can be applied to any relevant MapReduce task. Additionally, the user can specify the number of threads to utilize in the map and reduce stages.

## Running the Wordcount Example

The provided main.c uses the functions from mapreduce.c to implement a simple wordcount program.

Compilation:
gcc -o mapreduce main.c mapreduce.c hashmap.c -Wall -Werror -pthread

Run Command:
./mapreduce (input_library.txt) (Search String)

Examples:

./mapreduce basic.txt four      \n
should find "four" 4 times.

./mapreduce big.txt Nulla       \n
should find "Nulla" 828 times.

./mapreduce big.txt Nullam       \n
should find "Nullam" 643 times.
