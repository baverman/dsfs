cimport cython
from cython cimport view
from cpython cimport array

import array

from libc.math cimport log2

cdef array.array log2table = array.array('l', [0] * 65536)
cdef long *log2tablep = <long*>log2table.data.as_voidptr

lt = log2table


@cython.cdivision(True)
cpdef long jhash(unsigned long long key, long buckets) nogil:
    cdef long b = -1
    cdef long long j = 0
    while j < buckets:
        b = j
        key = key * 2862933555777941757ULL + 1
        j = <long long>((b + 1) * <double>(0x80000000) / <double>((key >> 33) + 1.0))

    return b


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef inline long straw(unsigned long long key, long[::view.contiguous] weights) nogil:
    cdef long maxidx = -1
    cdef long maxstraw = -2000000000L
    cdef long cstraw
    for i in range(weights.shape[0]):
        key = key * 2862933555777941757ULL + 1
        cstraw = log2tablep[key >> 48] / weights[i]
        if cstraw > maxstraw:
            maxstraw = cstraw
            maxidx = i

    return maxidx


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cdef void fill_table():
    cdef long i
    for i in range(65536):
        log2tablep[i] = <long>(log2(<double>(i + 1) / 65536.0) * 100000000)

fill_table()
