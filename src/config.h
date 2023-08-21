#ifndef __CONFIG_H
#define __CONFIG_H

#define redis_fstat fstat
#define redis_stat stat

#ifdef __linux__
#define HAVE_PROC_STAT 1
#define HAVE_PROC_MAPS 1
#define HAVE_PROC_SMAPS 1
#endif

#ifdef __linux__
#define HAVE_BACKTRACE 1
#define HAVE_EPOLL 1
#define aof_fsync fdatasync
#endif

#ifdef __linux__
#include <linux/version.h>
#include <features.h>
#if defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if (LINUX_VERSION_CODE >= 0x020611 && __GLIBC_PREREQ(2, 6))
#define HAVE_SYNC_FILE_RANGE 1
#endif
#endif
#endif

#ifdef HAVE_SYNC_FILE_RANGE
#define rdb_fsync_range(fd,off,size) sync_file_range(fd,off,size,SYNC_FILE_RANGE_WAIT_BEFORE|SYNC_FILE_RANGE_WRITE)
#endif

#ifdef __linux__
#define USE_SETPROCTITLE
#define INIT_SET_PROCTITLE_REPLACEMENT
void spt_init(int argc, char *argv[]);
void setproctitle(const char *fmt, ...);
#endif

#if (__i386 || __amd64) && __GNUC__
#define GNUC_VERSION (__GNUC__ * 10000 + __GNUC__MINOR__ * 100 + __GNUC_PATCHLEVEL__)
#if (GNUC_VERSION >= 40100 || defined(__clang__))
#define HAVE_ATOMIC
#endif
#endif

#endif