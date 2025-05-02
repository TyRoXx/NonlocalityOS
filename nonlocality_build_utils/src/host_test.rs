use crate::host::{add_executable_ending, HostOperatingSystem};

#[test_log::test]
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
