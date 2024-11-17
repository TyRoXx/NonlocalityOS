use std::fs::File;

#[cfg(any(unix, target_os = "wasi"))]
fn fd_to_object(file_descriptor: i32) -> File {
    use std::os::fd::FromRawFd;
    unsafe { File::from_raw_fd(file_descriptor) }
}

#[cfg(target_os = "windows")]
fn fd_to_object(_file_descriptor: i32) -> File {
    todo!();
}

#[cfg(target_arch = "wasm32")]
extern "C" {
    pub fn nonlocality_connect(interface: i32) -> i32;
    pub fn nonlocality_accept() -> u64;
    pub fn nonlocality_abort();
    pub fn nonlocality_tcp_ssl_handshake(host: *const u8, host_length: i32, port: i32) -> i32;
}

// dummy definition to make the linker happy on Linux
#[cfg(not(target_arch = "wasm32"))]
pub unsafe extern "C" fn nonlocality_connect(_interface: i32) -> i32 {
    panic!()
}

// dummy definition to make the linker happy on Linux
#[cfg(not(target_arch = "wasm32"))]
pub unsafe extern "C" fn nonlocality_accept() -> u64 {
    panic!()
}

// dummy definition to make the linker happy on Linux
#[cfg(not(target_arch = "wasm32"))]
pub unsafe extern "C" fn nonlocality_tcp_ssl_handshake(
    _host: *const u8,
    _host_length: i32,
    _port: i32,
) -> i32 {
    panic!()
}

pub struct Accepted {
    pub interface: i32,
    pub stream: File,
}

pub fn accept() -> std::io::Result<Accepted> {
    let encoded_result = unsafe { nonlocality_accept() };
    let interface = (encoded_result >> 32) as i32;
    let file_descriptor = (encoded_result & (u32::max_value() as u64)) as i32;
    if interface < 0 || file_descriptor < 0 {
        return Err(std::io::Error::new(std::io::ErrorKind::Other,
            "nonlocality_accept most likely failed because you tried to call it from two threads at the same time."));
    }
    let stream = fd_to_object(file_descriptor);
    Ok(Accepted { interface, stream })
}

pub fn connect(interface: i32) -> File {
    let file_descriptor = unsafe { nonlocality_connect(interface) };

    fd_to_object(file_descriptor)
}

pub fn tcp_ssl_handshake(host: &str, port: u16) -> std::io::Result<File> {
    let result =
        unsafe { nonlocality_tcp_ssl_handshake(host.as_ptr(), host.len() as i32, port as i32) };
    if result < 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "nonlocality_tcp_ssl_handshake failed and this error currently can't tell you why.",
        ));
    }
    let stream = fd_to_object(result);
    Ok(stream)
}
