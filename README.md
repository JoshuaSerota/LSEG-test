# LSEG-test
Interview assignment for my application to the London Stock Exchange Group.

# Design Summary

Because the scale of the project was fairly small, I have confined the code to a single file, LSEG-test.cpp. It consists primarily of two functions: the producer and consumer threads. 

The producer reads the input CSV file and processes it line by line into a queue of rows formatted more conveniently for the consumer.

The consumer reads all the data in the queue, organizes the data by which financial instrument the data is about, and then outputs the data to files. Each financial instrument has all its data consolidated and sorted in a corresponding single file.

The main function simply invokes these two threads. The data queue is shared between the two threads, with access mediated by a mutex. A condition variable is used to allow the producer thread to signal to the consumer thread when it has data available to consume. This avoids wasteful busy waiting.

The remainder of the source code file consists of a few minor utility functions and the struct definition for the data queue.

# Test Plan

More comprehensive testing would involve adding a number of different files for different test inputs. These should include the following test cases:

* Large data
    * Many rows
    * Large values for input fields
    * Long symbols
* Malformed data
    * Field type mismatches
    * Missing fields
    * Extra fields
    * Malicious content (such as symbols that attempt to trick the program into writing files to unsafe locations.)
