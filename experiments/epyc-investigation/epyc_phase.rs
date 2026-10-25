//! Experimental phase timing with a full-size arena, pinning, and start barrier.
//! Profiled-loop measurements guide hypotheses; the backend judges throughput.
use argon2::{Algorithm, Argon2, Params, Version};
use blocknet_pow_kernel::{CpuKernel, FixedArgon2id, PowBlock};
use std::io::{self, Write};
use std::time::{Duration, Instant};

fn main() {
    let a: Vec<String> = std::env::args().collect();
    assert_eq!(a.len(), 5, "epyc_phase KERNEL CPU PAGE_MODE SECONDS");
    let kernel: CpuKernel = a[1].parse().unwrap();
    let cpu: usize = a[2].parse().unwrap();
    let mode = a[3].as_str();
    let seconds: u64 = a[4].parse().unwrap();
    assert!((1..=120).contains(&seconds));
    assert!(cpu < libc::CPU_SETSIZE as usize);
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::CPU_SET(cpu, &mut set);
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set),
            0
        );
    }
    const MEMORY: u32 = 2 * 1024 * 1024;
    const BYTES: usize = MEMORY as usize * 1024;
    let salt = b"epyc-phase-full-reference";
    let password = 0u64.to_le_bytes();
    let mut reference = [0u8; 32];
    Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(MEMORY, 1, 1, Some(32)).unwrap(),
    )
    .hash_password_into(&password, salt, &mut reference)
    .unwrap();
    let hasher = FixedArgon2id::with_kernel(MEMORY, kernel).unwrap();
    let huge = match mode {
        "regular" => 0,
        "large" => libc::MAP_HUGETLB | (21 << 26),
        "large-1g" => libc::MAP_HUGETLB | (30 << 26),
        _ => panic!("page mode"),
    };
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            BYTES,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANON | huge,
            -1,
            0,
        )
    };
    assert_ne!(
        ptr,
        libc::MAP_FAILED,
        "mmap: {}",
        io::Error::last_os_error()
    );
    unsafe {
        if huge == 0 {
            assert_eq!(libc::madvise(ptr, BYTES, libc::MADV_NOHUGEPAGE), 0);
        }
        std::ptr::write_bytes(ptr.cast::<u8>(), 0, BYTES);
    }
    let memory = unsafe { std::slice::from_raw_parts_mut(ptr.cast::<PowBlock>(), MEMORY as usize) };
    let mut output = [0u8; 32];
    hasher
        .profile_split_hash_with_memory(&password, salt, &mut output, memory)
        .unwrap();
    assert_eq!(
        output, reference,
        "full-size profiled path reference mismatch"
    );
    // Dirty-arena reuse must agree with the normal path, too.
    hasher
        .hash_password_into_with_memory(&password, salt, &mut output, memory)
        .unwrap();
    assert_eq!(output, reference);
    println!("READY cpu={cpu} kernel={kernel} mode={mode} arena_bytes={BYTES} parity=PASS");
    io::stdout().flush().unwrap();
    let mut line = String::new();
    io::stdin().read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "START");
    let started = Instant::now();
    let mut count = 0u64;
    let mut seed = Duration::ZERO;
    let mut di = Duration::ZERO;
    let mut dd = Duration::ZERO;
    let mut finalize = Duration::ZERO;
    while started.elapsed() < Duration::from_secs(seconds) {
        count += 1;
        // Match independent mining streams across workers rather than using
        // identical data-dependent address sequences in every process.
        let nonce = ((cpu as u64) << 32) | count;
        let timing = hasher
            .profile_split_hash_with_memory(&nonce.to_le_bytes(), salt, &mut output, memory)
            .unwrap();
        seed += timing.seed;
        di += timing.data_independent;
        dd += timing.data_dependent;
        finalize += timing.finalize;
        std::hint::black_box(&output);
    }
    let elapsed = started.elapsed().as_secs_f64();
    println!("{{\"cpu\":{cpu},\"kernel\":\"{kernel}\",\"mode\":\"{mode}\",\"hashes\":{count},\"elapsed\":{elapsed},\"seed\":{},\"di\":{},\"dd\":{},\"finalize\":{}}}",
        seed.as_secs_f64(), di.as_secs_f64(), dd.as_secs_f64(), finalize.as_secs_f64());
    assert_eq!(unsafe { libc::munmap(ptr, BYTES) }, 0);
}
