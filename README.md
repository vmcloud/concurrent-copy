# conc-copy
conc-copy is a file-copy tool for linux, which is like cp command, but it supports setting multiple threads to do the copy work, which is very useful in the scenario where the throughput of storage system is not the bottleneck of data transferring.

### Build
```sh
$ make
```
### Install
```sh
$ sudo make install
```
### Usage
```sh
conc-copy [-f] [-r] [-t thread_num] SOURCE DEST
```
