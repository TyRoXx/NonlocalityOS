#[derive(Clone, Copy)]
pub enum HostOperatingSystem {
    WindowsAmd64,
    LinuxAmd64,
}

pub fn detect_host_operating_system() -> HostOperatingSystem {
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    return HostOperatingSystem::WindowsAmd64;
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    return HostOperatingSystem::LinuxAmd64;
}

pub fn add_executable_ending(host: &HostOperatingSystem, base_name: &str) -> String {
    match host {
        HostOperatingSystem::WindowsAmd64 => format!("{}.exe", base_name),
        HostOperatingSystem::LinuxAmd64 => base_name.to_string(),
    }
}

#[test]
fn test_add_executable_ending() {
    assert_eq!(
        "",
        add_executable_ending(&HostOperatingSystem::LinuxAmd64, "")
    );
    assert_eq!(
        ".exe",
        add_executable_ending(&HostOperatingSystem::WindowsAmd64, "")
    );
    assert_eq!(
        "aaa",
        add_executable_ending(&HostOperatingSystem::LinuxAmd64, "aaa")
    );
    assert_eq!(
        "aaa.exe",
        add_executable_ending(&HostOperatingSystem::WindowsAmd64, "aaa")
    );
}
