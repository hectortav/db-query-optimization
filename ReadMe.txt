How to compile and run the main program
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