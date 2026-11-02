//! Installed memory modules from the SMBIOS type 17 table, for startup hints about
//! desktop memory setups: one module (single channel) or RAM running below its
//! rated or EXPO/XMP speed. Windows reads the
//! table without privileges; Linux only as root (`/sys/firmware/dmi/tables/DMI`).

const TYPE_MEMORY_DEVICE: u8 = 17;
const TYPE_END: u8 = 127;
const FORM_DIMM: u8 = 0x09;
const FORM_SODIMM: u8 = 0x0d;
const MEMORY_DDR4: u8 = 0x1a;
const MEMORY_DDR5: u8 = 0x22;
const DETAIL_REGISTERED: u16 = 1 << 13;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryModule {
    pub size_mib: u64,
    pub form_factor: u8,
    pub memory_type: u8,
    pub registered: bool,
    /// Rated speed in MT/s (SMBIOS "Speed"; often the JEDEC speed for EXPO/XMP kits).
    pub rated_mts: u32,
    /// Speed the firmware configured, in MT/s.
    pub configured_mts: u32,
}

impl MemoryModule {
    fn generation(&self) -> Option<&'static str> {
        match self.memory_type {
            MEMORY_DDR4 => Some("DDR4"),
            MEMORY_DDR5 => Some("DDR5"),
            _ => None,
        }
    }

    /// SMBIOS memory type name for telemetry, covering the types miners run.
    fn type_name(&self) -> Option<&'static str> {
        match self.memory_type {
            0x18 => Some("DDR3"),
            0x1d => Some("LPDDR3"),
            0x1e => Some("LPDDR4"),
            0x23 => Some("LPDDR5"),
            _ => self.generation(),
        }
    }
}

/// Telemetry summary of the populated modules; `None` when the table is
/// unreadable or lists none.
pub fn host_memory_setup() -> Option<seine_telemetry::MemorySetup> {
    summarize(&parse_memory_modules(&smbios_table()?))
}

fn summarize(modules: &[MemoryModule]) -> Option<seine_telemetry::MemorySetup> {
    let first = modules.first()?;
    let lowest =
        |speed: fn(&MemoryModule) -> u32| modules.iter().map(speed).filter(|&v| v > 0).min();
    Some(seine_telemetry::MemorySetup {
        modules: modules.len().min(1024) as u32,
        memory_type: first.type_name().map(str::to_owned),
        registered: modules.iter().any(|m| m.registered),
        rated_mts: lowest(|m| m.rated_mts).filter(|&v| v <= 100_000),
        configured_mts: lowest(|m| m.configured_mts).filter(|&v| v <= 100_000),
    })
}

/// Populated memory devices in a raw SMBIOS structure table.
pub fn parse_memory_modules(table: &[u8]) -> Vec<MemoryModule> {
    let mut modules = Vec::new();
    let mut at = 0usize;
    while at + 4 <= table.len() {
        let kind = table[at];
        let len = table[at + 1] as usize;
        if len < 4 || at + len > table.len() {
            break;
        }
        let formatted = &table[at..at + len];
        if kind == TYPE_MEMORY_DEVICE {
            if let Some(module) = memory_device(formatted) {
                modules.push(module);
            }
        }
        // Skip the string set, which ends with two NUL bytes.
        let mut next = at + len;
        while next + 1 < table.len() && !(table[next] == 0 && table[next + 1] == 0) {
            next += 1;
        }
        at = next + 2;
        if kind == TYPE_END {
            break;
        }
    }
    modules
}

fn memory_device(s: &[u8]) -> Option<MemoryModule> {
    let u16_at = |off: usize| {
        s.get(off..off + 2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]))
    };
    let u32_at = |off: usize| {
        s.get(off..off + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    };
    let size = u16_at(0x0c)?;
    let size_mib = match size {
        0 | 0xffff => return None,
        0x7fff => u64::from(u32_at(0x1c)? & 0x7fff_ffff),
        v if v & 0x8000 != 0 => u64::from(v & 0x7fff) / 1024,
        v => u64::from(v),
    };
    if size_mib == 0 {
        return None;
    }
    let speed = |off: usize, extended: usize| match u16_at(off) {
        Some(0xffff) => u32_at(extended).unwrap_or(0),
        Some(v) => u32::from(v),
        None => 0,
    };
    Some(MemoryModule {
        size_mib,
        form_factor: *s.get(0x0e)?,
        memory_type: *s.get(0x12)?,
        registered: u16_at(0x13).is_some_and(|d| d & DETAIL_REGISTERED != 0),
        rated_mts: speed(0x15, 0x54),
        configured_mts: speed(0x20, 0x58),
    })
}

/// One-line hints for a desktop memory setup that limits CPU hashrate. Only
/// unbuffered DDR4/DDR5 DIMMs and SO-DIMMs qualify, so servers (registered),
/// soldered laptop memory and unknown tables stay quiet.
pub fn memory_setup_notices(modules: &[MemoryModule]) -> Vec<String> {
    let desktop = !modules.is_empty()
        && modules.iter().all(|m| {
            !m.registered
                && matches!(m.form_factor, FORM_DIMM | FORM_SODIMM)
                && m.generation().is_some()
        });
    if !desktop {
        return Vec::new();
    }
    let generation = modules[0].generation().unwrap_or("RAM");
    let configured = modules.iter().map(|m| m.configured_mts).min().unwrap_or(0);
    let rated = modules.iter().map(|m| m.rated_mts).min().unwrap_or(0);
    let mut notices = Vec::new();
    if modules.len() == 1 {
        notices.push(format!(
            "only one memory module is installed ({} GiB {generation}); a second matching module doubles the memory bandwidth CPU mining depends on",
            modules[0].size_mib.div_ceil(1024)
        ));
    }
    if configured > 0 && rated > 0 && u64::from(configured) * 100 < u64::from(rated) * 95 {
        notices.push(format!(
            "memory runs at {configured} MT/s but is rated {rated} MT/s; enable EXPO/XMP in BIOS for more CPU hashrate"
        ));
    } else if configured > 0
        && ((generation == "DDR5" && configured <= 4800)
            || (generation == "DDR4" && configured <= 2666))
    {
        notices.push(format!(
            "memory runs at the {generation} default of {configured} MT/s; enabling EXPO/XMP in BIOS may raise CPU hashrate"
        ));
    }
    notices
}

/// Startup hints for this host's memory setup; empty when the table is unreadable.
pub fn host_memory_setup_notices() -> Vec<String> {
    smbios_table()
        .map(|table| memory_setup_notices(&parse_memory_modules(&table)))
        .unwrap_or_default()
}

#[cfg(target_os = "linux")]
fn smbios_table() -> Option<Vec<u8>> {
    std::fs::read("/sys/firmware/dmi/tables/DMI").ok()
}

#[cfg(target_os = "windows")]
fn smbios_table() -> Option<Vec<u8>> {
    use windows_sys::Win32::System::SystemInformation::GetSystemFirmwareTable;
    let provider = u32::from_be_bytes(*b"RSMB");
    // SAFETY: a null buffer with size 0 only queries the required size.
    let needed = unsafe { GetSystemFirmwareTable(provider, 0, std::ptr::null_mut(), 0) };
    if needed <= 8 {
        return None;
    }
    let mut raw = vec![0u8; needed as usize];
    // SAFETY: the buffer holds `needed` bytes.
    let got = unsafe { GetSystemFirmwareTable(provider, 0, raw.as_mut_ptr().cast(), needed) };
    if got != needed {
        return None;
    }
    // RawSMBIOSData: 4 version bytes, a u32 length, then the structure table.
    let len = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;
    raw.get(8..8 + len).map(<[u8]>::to_vec)
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
fn smbios_table() -> Option<Vec<u8>> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A type 17 structure (SMBIOS 3.x layout, 0x5c bytes) plus an empty string set.
    fn device(
        size_mib: u16,
        form: u8,
        kind: u8,
        detail: u16,
        rated: u16,
        configured: u16,
    ) -> Vec<u8> {
        let mut s = vec![0u8; 0x5c];
        s[0] = TYPE_MEMORY_DEVICE;
        s[1] = 0x5c;
        s[0x0c..0x0e].copy_from_slice(&size_mib.to_le_bytes());
        s[0x0e] = form;
        s[0x12] = kind;
        s[0x13..0x15].copy_from_slice(&detail.to_le_bytes());
        s[0x15..0x17].copy_from_slice(&rated.to_le_bytes());
        s[0x20..0x22].copy_from_slice(&configured.to_le_bytes());
        s.extend_from_slice(&[0, 0]);
        s
    }

    fn table(devices: &[Vec<u8>]) -> Vec<u8> {
        let mut t: Vec<u8> = devices.concat();
        t.extend_from_slice(&[TYPE_END, 4, 0, 0, 0, 0]);
        t
    }

    #[test]
    fn parses_populated_devices_and_skips_empty_slots() {
        let t = table(&[
            device(16384, FORM_DIMM, MEMORY_DDR5, 1 << 14, 4800, 6000),
            device(0, FORM_DIMM, MEMORY_DDR5, 0, 0, 0),
            device(16384, FORM_DIMM, MEMORY_DDR5, 1 << 14, 4800, 6000),
        ]);
        let modules = parse_memory_modules(&t);
        assert_eq!(modules.len(), 2);
        assert_eq!(modules[0].size_mib, 16384);
        assert_eq!(
            (modules[0].rated_mts, modules[0].configured_mts),
            (4800, 6000)
        );
        assert!(!modules[0].registered);
        assert!(
            memory_setup_notices(&modules).is_empty(),
            "dual channel with EXPO on"
        );
    }

    #[test]
    fn single_module_and_jedec_speed_are_flagged() {
        let modules = parse_memory_modules(&table(&[device(
            16384,
            FORM_DIMM,
            MEMORY_DDR5,
            1 << 14,
            4800,
            4800,
        )]));
        let notices = memory_setup_notices(&modules);
        assert_eq!(notices.len(), 2);
        assert!(notices[0].contains("only one memory module"));
        assert!(notices[1].contains("DDR5 default of 4800 MT/s"));
    }

    #[test]
    fn below_rated_speed_is_flagged() {
        let modules = parse_memory_modules(&table(&[
            device(8192, FORM_SODIMM, MEMORY_DDR4, 1 << 14, 3200, 2400),
            device(8192, FORM_SODIMM, MEMORY_DDR4, 1 << 14, 3200, 2400),
        ]));
        let notices = memory_setup_notices(&modules);
        assert_eq!(notices.len(), 1);
        assert!(notices[0].contains("2400 MT/s but is rated 3200"));
    }

    #[test]
    fn servers_and_unknown_memory_stay_quiet() {
        let registered = parse_memory_modules(&table(&[device(
            16384,
            FORM_DIMM,
            MEMORY_DDR5,
            DETAIL_REGISTERED,
            4800,
            4800,
        )]));
        assert!(memory_setup_notices(&registered).is_empty());
        let soldered = parse_memory_modules(&table(&[device(16384, 0x0b, 0x23, 0, 6400, 6400)]));
        assert!(memory_setup_notices(&soldered).is_empty());
        assert!(memory_setup_notices(&[]).is_empty());
        assert!(parse_memory_modules(&[17, 2]).is_empty());
    }

    #[test]
    fn telemetry_summary_uses_lowest_known_speeds() {
        let modules = parse_memory_modules(&table(&[
            device(16384, FORM_DIMM, MEMORY_DDR5, 1 << 14, 4800, 6000),
            device(16384, FORM_DIMM, MEMORY_DDR5, 1 << 14, 0, 5600),
        ]));
        let setup = summarize(&modules).unwrap();
        assert_eq!(setup.modules, 2);
        assert_eq!(setup.memory_type.as_deref(), Some("DDR5"));
        assert!(!setup.registered);
        assert_eq!(
            (setup.rated_mts, setup.configured_mts),
            (Some(4800), Some(5600))
        );
        assert!(summarize(&[]).is_none());
    }

    #[test]
    fn extended_size_and_speed_fields() {
        let mut d = device(0x7fff, FORM_DIMM, MEMORY_DDR5, 1 << 14, 0xffff, 0xffff);
        d[0x1c..0x20].copy_from_slice(&65536u32.to_le_bytes());
        d[0x54..0x58].copy_from_slice(&8000u32.to_le_bytes());
        d[0x58..0x5c].copy_from_slice(&7200u32.to_le_bytes());
        let modules = parse_memory_modules(&table(&[d]));
        assert_eq!(modules[0].size_mib, 65536);
        assert_eq!(
            (modules[0].rated_mts, modules[0].configured_mts),
            (8000, 7200)
        );
    }
}
