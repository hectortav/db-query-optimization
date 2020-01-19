# Software Development forInformation Systems

[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)
# How to compile and run the main program
----------------------------------
make
./final

How to compile and run the unit testing program
------------------------------------------
cd tests
make
./main

How to install CUnit on Linux
-----------------------------
To install CUnit on debian squeeze:
sudo apt-get install libcunit1-dev libcunit1-doc libcunit1

To install on RPM based systems, use one of the RPM's on the net appropriate for your distro.

To install by hand:
apt-get install autoconf
Install CUnit, CUnit is built as a static library which is linked with the user's test code.
Download the source code from http://sourceforge.net/projects/cunit/
cd CUnit-2.1-0xx
chmod u+x configure (if necessary)
./configure
make
make install

# Introduction

The following study was executed as part of the course on software
development for information systems in the <span>*Notional and
Kapodistrian University of Athens*</span>. The aim of this study was to
create a subset of a database that manages data entirely in the main
memory. The whole task was split in three parts. In the first part we
implemented a Sort Merge Join Algorithm, in the second one we performed
a query analysis, similar to the one used in the <span>*SIGMOD
2018*</span> competition, while in the last part we implemented CPU
personalization and a query optimizer. The project is hosted in GitHub
(https://github.com/hectortav/Project2020) and contains a Makefile for
compilation and a bash script (testProgram.sh) to easily test the
program. To run the executable, produced by the Makefile, called
"final", you can use one or more of the following flags to provide cli
arguments:

  - \-qr (QueRy) Run queries of every batch in parallel

  - \-ro (ReOrder) Run bucket reorder (radix-sort) in parallel

  - \-pb Create a new parallel job for each new bucket

  - \-qs (QuickSort) Run quicksorts independently

  - \-jn (JoiN) Run join in parallel

  - \-ft (FilTer) Run filters in parallel

  - \-pj (ProJection) Run Projection checksums in parallel

  - \-all (ALL) Everything runs in parallel

  - \-n "threads" Specify number of threads to run

  - \-jnthreads Complementary way to manage Join

# Observations and choices during implementation

We implemented and tested two different methods for sorting buckets
during the first and second part. One of them was using recursion while
the other one was using a loop to break the buckets into smaller ones
when needed. After thorough testing we decided to use the recursive one
since we managed to achieve better execution times and convolution while
we also managed to keep each thread work on a different memory part at
any time, reducing the need for semaphores which would slow the
execution further since each thread would have to wait to access the
memory.

Another part of the execution that we tried to parallelize was the copy
from one tuple to another during each call of the Reorder function
(tuplereorder\_parallel @ functions.cpp), but we finally decided not to
keep the change since it significantly slowed the program because all
the extra threads that would be used to execute the copy could be used
more efficiently by another function that is waiting in the scheduler.

To adapt Join for running on a parallel processing system, we tried
several different implementations. First, we tried the one described in
class, that is to join areas that have the same prefix. Then, we tried
merging some of these areas together so that each new area cluster is
assigned to a thread. Finally, we tried to split the relation in equal
parts so that each part is assigned to a thread. We noticed that the
third method that kept the job count at a minimum while at the same time
accomplished a balanced load between threads, produced a better result
concerning time and processor usage.

We noticed that we had great time improvements when we used smaller and
continues memory allocations. For example by turning histogram and psum
from 2D arrays to 1D we managed to cut the execution time to 20% of what
we had with the model described in class. The reasoning behind this
great improvement is that a big part of the array can be kept in cache
and we don’t have to worry about loading times that take a long time.

Another observation was the difference between the small and medium
inputs. We noticed that the multi threading was better exploited with
the medium sized input, where we saw greater time and CPU usage
improvements. The explanation is that the time that it takes to split
the jobs and wait for an available thread is better justified when the
inputs are greater and the time relation of the thread organization
processes to the time of the rest of the processes tend to zero.

While running the program through a profiler, we noticed that
partitioning during predicate optimization, consumed 37% of the overall
time. That means there are many data in a small number of buckets. That
way we do not have the optimal outcome we could accomplish with an other
optimization policy.

Another observation we made is that the list functions (e.g. insert)
consume 17% of the time. If we had another data structure that would not
require constant checks and processes to allocate new memory blocks we
would achieve better times with probably an allocation of unneeded
memory as a negative aspect.
# Parallelization Observations

At the third and last part of the project, parallelization was added to
the program. The effect this transition had on the program is
immediately apparent when looking at the overall execution times. Adding
CPU parallelization led to a decline of 20% concerning execution time
and a increase of 70% concerning CPU usage which means computer
resources are better utilized and the overall outcome is maximized.

# Version Control Observations

During the whole project we used git as a version control system. With
git we managed to track bugs that emerged during development and then
easily solve them. Git also helped our team to share code and
communicate better that led to better organization and management.
Finally, a git aspect we found greatly useful was branching since each
team member could work in separate features without causing problems and
interference.

# Unit Testing

To test the execution of our program we used unit testing and especially
Cunit. We tried to make unit tests for every function regardless of the
size or simplicity. Some of the functions tested are the following:

- randomIndex

- swap

- hashFunction

- makeparts

- splitpreds

- optimizepredicates

- predsplittoterms

- sortBucket

- histcreateTest

- psumcreateTest

- tuplesReorderTest

- InputArray::filterRowIds

- InputArray::extractColumnFromRowIds

- IntermediateArray::populate

- IntermediateArray::findColumnIndexByInputArrayId

- IntermediateArray::findColumnIndexByPredicateArrayId

- IntermediateArray::selfJoin

Overall we concluded that unit testing in an easy way to test and
validate the solidity of a big project, that would otherwise require
many hours of manual testing. Also we found that creating unit tests
before the project is a valid way to ensure the smooth development of a
program as well as keeping the team in track and necessitate the usage
of a common API.

# Conclusion

During this assignment we used various technologies and researched a
plethora of ideas and theories. We appreciated concepts like parallel
programming, unit testing, version control and of course software
development for information systems. Parallel programming is a useful
asset today since all systems have multiple real and virtual CPU cores
that are not often utilized correctly. Unit testing is a respectful way
to ensure that programs & algorithms are valid and ready for production
and version control is an easy way to track bugs and work with teams.
Information systems and databases are the core of today’s computing and
leaving in an age where data are abundant, we must explore new and more
efficient ways to store, find and index them. Last but not least, we
learned the importance of pre-planning, estimating possible obstacles
and chances during development of such last projects.
