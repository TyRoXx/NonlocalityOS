# Thumbnail Caching in dogbox_dav_server

## Overview

The dogbox_dav_server WebDAV server provides built-in support for thumbnail caching through standard HTTP/WebDAV mechanisms. This enables file explorers on Windows and Linux to cache thumbnails and avoid recalculating them on every access.

## How It Works

### ETag Support

The server automatically generates ETags (Entity Tags) for all files based on:
- File size (in bytes)
- Last modification time (in microseconds since UNIX epoch)

The ETag format is: `"{size:x}-{mtime:x}"` for files with content, or just `"{mtime:x}"` for empty files.

**Example ETags:**
- `"d-c65d40"` - A 13-byte file (0xd) modified at 13,000,000 microseconds (0xc65d40) after epoch
- `"1b-c65d40"` - A 27-byte file (0x1b) modified at the same time

### ETag Stability

ETags remain stable as long as the file content and modification time don't change. This is crucial for thumbnail caching:

1. **Unchanged files** → Same ETag → Client can use cached thumbnail
2. **Modified files** → Different ETag → Client knows to regenerate thumbnail

### Last-Modified Headers

In addition to ETags, the server provides accurate `Last-Modified` headers based on the file's modification time stored in the DogBox tree structure.

## Client Behavior

### Windows Explorer

Windows Explorer (and other Windows file managers) use WebDAV thumbnail caching by:
1. Downloading the file and generating a thumbnail
2. Storing the thumbnail in the Windows thumbnail cache along with the file's ETag
3. On subsequent accesses, checking if the ETag has changed
4. If the ETag is unchanged, using the cached thumbnail
5. If the ETag changed, regenerating the thumbnail

### Linux File Managers (Nautilus, Dolphin, Thunar)

Linux file managers following the [FreeDesktop.org thumbnail specification](https://specifications.freedesktop.org/thumbnail-spec/latest/) cache thumbnails by:
1. Creating a thumbnail and storing it in `~/.cache/thumbnails/`
2. Storing the file's URI and modification time (mtime) in the thumbnail's metadata
3. On subsequent accesses, comparing the current mtime with the cached mtime
4. If unchanged, using the cached thumbnail
5. If changed, regenerating the thumbnail

The WebDAV server's accurate modification times enable this caching to work correctly.

## Implementation Details

### File Metadata Types

The server implements three metadata types, all providing ETag support through the `DavMetaData` trait:

1. **DogBoxMetaData** - For files and directories from tree entries
2. **DogBoxFileMetaData** - For open files being accessed
3. **DogBoxDirectoryMetaData** - For directory listings

All three types implement:
- `len()` - Returns file size
- `modified()` - Returns modification time
- `is_dir()` - Returns whether it's a directory

The `DavMetaData` trait provides a default `etag()` implementation that uses these methods to generate consistent ETags.

### ETag Generation

ETags are generated automatically by the `dav-server` library using the metadata:

```rust
fn etag(&self) -> Option<String> {
    if let Ok(t) = self.modified()
        && let Ok(t) = t.duration_since(UNIX_EPOCH)
    {
        let t = t.as_secs() * 1000000 + t.subsec_nanos() as u64 / 1000;
        let tag = if self.is_file() && self.len() > 0 {
            format!("{:x}-{:x}", self.len(), t)
        } else {
            format!("{t:x}")
        };
        return Some(tag);
    }
    None
}
```

### HTTP Headers

The server automatically includes these headers in HTTP responses:
- `ETag: "d-c65d40"` - The entity tag for cache validation
- `Last-Modified: Thu, 01 Jan 1970 00:00:13 GMT` - The last modification time

## Testing

The test `test_etag_stability_for_thumbnail_caching` in `lib_tests.rs` verifies:
1. ETags are present on all files
2. ETags remain stable when files don't change
3. ETags change when files are modified
4. The modified ETag is also stable

## Benefits

1. **Reduced bandwidth** - Clients don't need to re-download files to check if thumbnails are still valid
2. **Reduced server load** - Fewer file accesses and data transfers
3. **Faster user experience** - File explorers load faster with cached thumbnails
4. **Automatic** - No configuration needed, works out of the box

## Compatibility

This implementation follows standard HTTP/WebDAV protocols and is compatible with:
- Windows Explorer (Windows 7+)
- Windows File Explorer (Windows 8+)
- Nautilus (GNOME Files)
- Dolphin (KDE)
- Thunar (XFCE)
- Any WebDAV client that supports ETags and Last-Modified headers

## See Also

- [RFC 7232 - HTTP Conditional Requests](https://tools.ietf.org/html/rfc7232)
- [RFC 4918 - WebDAV](https://tools.ietf.org/html/rfc4918)
- [Windows Thumbnail Cache](https://en.wikipedia.org/wiki/Windows_thumbnail_cache)
- [FreeDesktop.org Thumbnail Specification](https://specifications.freedesktop.org/thumbnail-spec/latest/)
