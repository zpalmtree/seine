"""Linux x86-64 grouped per-process user-space PMCs for a waiting worker.

Includes only the interval after preparation. Counts include small post-loop
userspace reporting/exit overhead. Raw-event meanings are archived separately.
"""
import ctypes
import os
import struct

LIBC = ctypes.CDLL(None, use_errno=True)


class Attr(ctypes.Structure):
    _fields_ = [('type', ctypes.c_uint32), ('size', ctypes.c_uint32),
                ('config', ctypes.c_uint64), ('sample_period', ctypes.c_uint64),
                ('sample_type', ctypes.c_uint64), ('read_format', ctypes.c_uint64),
                ('flags', ctypes.c_uint64), ('wakeup_events', ctypes.c_uint32),
                ('bp_type', ctypes.c_uint32), ('config1', ctypes.c_uint64)]


class Group:
    def __init__(self, pid, events):
        self.fds = []
        self.events = events
        assert os.uname().machine == 'x86_64' and ctypes.sizeof(Attr) == 64
        try:
            for name, event_type, config in events:
                attr = Attr(type=event_type, size=64, config=config, read_format=11,
                            flags=(1 << 0) | (1 << 5) | (1 << 6))
                fd = LIBC.syscall(ctypes.c_long(298), ctypes.byref(attr), ctypes.c_int(pid),
                                  ctypes.c_int(-1), ctypes.c_int(self.fds[0] if self.fds else -1),
                                  ctypes.c_ulong(8))  # PERF_FLAG_FD_CLOEXEC
                if fd < 0:
                    raise OSError(ctypes.get_errno(), f'perf_event_open: {name}')
                self.fds.append(fd)
        except BaseException:
            self.close()
            raise

    def enable(self):
        assert LIBC.ioctl(self.fds[0], 0x2403, 1) == 0  # RESET, GROUP
        assert LIBC.ioctl(self.fds[0], 0x2400, 1) == 0  # ENABLE, GROUP

    def read(self):
        values = struct.unpack('=' + 'Q' * (3 + len(self.fds)),
                               os.read(self.fds[0], 8 * (3 + len(self.fds))))
        n, enabled, running, *counts = values
        assert n == len(self.fds) and enabled > 0
        assert running / enabled >= .98, ('multiplexed/unavailable', enabled, running)
        return {'enabled_ns': enabled, 'running_ns': running,
                'counts': dict(zip((e[0] for e in self.events), counts))}

    def close(self):
        for fd in self.fds:
            os.close(fd)
        self.fds.clear()


EVENTS = {
    'core': [('cycles', 0, 0), ('instructions', 0, 1),
             ('l2_dc_misses', 4, 0x0864), ('dtlb_walks', 4, 0xf045)],
    'requests': [('cycles', 0, 0), ('instructions', 0, 1),
                 ('l2_dc_reads', 4, 0x8060), ('l2_dc_stores', 4, 0x4060)],
    'fills': [('cycles', 0, 0), ('instructions', 0, 1),
              ('any_dram_io', 4, 0x4844), ('sw_prefetch_dram_io', 4, 0x4859)],
    'stores': [('cycles', 0, 0), ('instructions', 0, 1),
               ('l2_dc_stores', 4, 0x4060), ('wcb_full_lines', 4, 0x0150)],
}
