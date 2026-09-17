use std::panic;
use std::slice;

// Status codes
const STATUS_OK: i32 = 0;
const STATUS_INVALID_INPUT: i32 = -1;
const STATUS_INVALID_ARG: i32 = -6;
const STATUS_PANIC: i32 = -5;

// Format version tag (1 byte prepended to all stored values)
const FORMAT_VERSION: u8 = 1;

// Maximum nesting depth (default from Java: 1000)
const MAX_NESTING_DEPTH: usize = 1000;

/// Check nesting depth of JSON text without full parsing.
/// Returns true if depth is within limit, false otherwise.
fn check_nesting_depth(text: &str) -> bool {
    let mut depth = 0;
    let mut max_depth = 0;
    let mut in_string = false;
    let mut escape_next = false;

    for ch in text.chars() {
        if escape_next {
            escape_next = false;
            continue;
        }

        match ch {
            '\\' if in_string => escape_next = true,
            '"' => in_string = !in_string,
            '{' | '[' if !in_string => {
                depth += 1;
                if depth > max_depth {
                    max_depth = depth;
                    if max_depth > MAX_NESTING_DEPTH {
                        return false;
                    }
                }
            }
            '}' | ']' if !in_string => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            _ => {}
        }
    }

    true
}

/// Parse JSON text to binary JSONB.
/// Returns STATUS_OK on success, negative status code on error.
/// On success, sets out_ptr and out_len to the allocated result buffer.
/// Caller must free the result with jsonb_free.
#[no_mangle]
pub extern "C" fn jsonb_from_text(
    in_ptr: *const u8,
    in_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| {
        // Validate input pointers
        if in_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
            return STATUS_INVALID_ARG;
        }

        // Safety: caller guarantees in_ptr points to in_len valid bytes
        let input = unsafe { slice::from_raw_parts(in_ptr, in_len) };

        // Convert to UTF-8 string
        let text = match std::str::from_utf8(input) {
            Ok(s) => s,
            Err(_) => return STATUS_INVALID_INPUT,
        };

        // Check nesting depth before parsing
        if !check_nesting_depth(text) {
            return STATUS_INVALID_INPUT;
        }

        // Parse JSON text to JSONB binary
        let jsonb_value = match jsonb::parse_value(text.as_bytes()) {
            Ok(v) => v,
            Err(_) => return STATUS_INVALID_INPUT,
        };

        // Convert to binary format
        let binary = jsonb_value.to_vec();

        // Prepend format version tag
        let mut result_bytes = Vec::with_capacity(1 + binary.len());
        result_bytes.push(FORMAT_VERSION);
        result_bytes.extend_from_slice(&binary);

        // Leak the buffer and return pointer and length
        let boxed = result_bytes.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;

        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }

        STATUS_OK
    });

    result.unwrap_or(STATUS_PANIC)
}

/// Render binary JSONB to JSON text.
/// Returns STATUS_OK on success, negative status code on error.
/// On success, sets out_ptr and out_len to the allocated result buffer.
/// Caller must free the result with jsonb_free.
#[no_mangle]
pub extern "C" fn jsonb_to_text(
    in_ptr: *const u8,
    in_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| {
        // Validate input pointers
        if in_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
            return STATUS_INVALID_ARG;
        }

        if in_len == 0 {
            return STATUS_INVALID_INPUT;
        }

        // Safety: caller guarantees in_ptr points to in_len valid bytes
        let input = unsafe { slice::from_raw_parts(in_ptr, in_len) };

        // Check format version
        if input[0] != FORMAT_VERSION {
            return STATUS_INVALID_INPUT;
        }

        // Skip format version byte
        let binary = &input[1..];

        // Parse binary JSONB
        let jsonb_value = match jsonb::from_slice(binary) {
            Ok(v) => v,
            Err(_) => return STATUS_INVALID_INPUT,
        };

        // Convert to JSON text
        let text = jsonb_value.to_string();
        let text_bytes = text.into_bytes();

        // Leak the buffer and return pointer and length
        let boxed = text_bytes.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;

        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }

        STATUS_OK
    });

    result.unwrap_or(STATUS_PANIC)
}

/// Validate binary JSONB.
/// Returns STATUS_OK if valid, STATUS_INVALID_INPUT if malformed.
#[no_mangle]
pub extern "C" fn jsonb_validate(
    in_ptr: *const u8,
    in_len: usize,
) -> i32 {
    let result = panic::catch_unwind(|| {
        // Validate input pointer
        if in_ptr.is_null() {
            return STATUS_INVALID_ARG;
        }

        if in_len == 0 {
            return STATUS_INVALID_INPUT;
        }

        // Safety: caller guarantees in_ptr points to in_len valid bytes
        let input = unsafe { slice::from_raw_parts(in_ptr, in_len) };

        // Check format version
        if input[0] != FORMAT_VERSION {
            return STATUS_INVALID_INPUT;
        }

        // Skip format version byte
        let binary = &input[1..];

        // Validate by attempting to parse
        match jsonb::from_slice(binary) {
            Ok(_) => STATUS_OK,
            Err(_) => STATUS_INVALID_INPUT,
        }
    });

    result.unwrap_or(STATUS_PANIC)
}

/// Free a buffer allocated by jsonb_from_text or jsonb_to_text.
/// Must be called exactly once for each non-null result buffer.
#[no_mangle]
pub extern "C" fn jsonb_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        // Safety: ptr was allocated by Box::into_raw with the given length
        unsafe {
            let _ = Box::from_raw(slice::from_raw_parts_mut(ptr, len) as *mut [u8]);
        }
    }
}

// Additional status codes
const STATUS_NOT_FOUND: i32 = -3;

/// Get a JSONB value by key from an object.
/// Returns the value as JSONB, or NOT_FOUND if key doesn't exist.
#[no_mangle]
pub extern "C" fn jsonb_get_by_key(
    in_ptr: *const u8,
    in_len: usize,
    key_ptr: *const u8,
    key_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if in_ptr.is_null() || key_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
            return STATUS_INVALID_ARG;
        }

        let input = unsafe { slice::from_raw_parts(in_ptr, in_len) };
        let key_bytes = unsafe { slice::from_raw_parts(key_ptr, key_len) };

        if input.is_empty() || input[0] != FORMAT_VERSION {
            return STATUS_INVALID_INPUT;
        }

        let key = match std::str::from_utf8(key_bytes) {
            Ok(s) => s,
            Err(_) => return STATUS_INVALID_INPUT,
        };

        let jsonb_value = match jsonb::from_slice(&input[1..]) {
            Ok(v) => v,
            Err(_) => return STATUS_INVALID_INPUT,
        };

        let object = match jsonb_value.as_object() {
            Some(obj) => obj,
            None => return STATUS_INVALID_INPUT,
        };

        let result_value = match object.get(key) {
            Some(v) => v,
            None => return STATUS_NOT_FOUND,
        };

        let binary = result_value.to_vec();
        let mut result_bytes = Vec::with_capacity(1 + binary.len());
        result_bytes.push(FORMAT_VERSION);
        result_bytes.extend_from_slice(&binary);

        let boxed = result_bytes.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;

        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }

        STATUS_OK
    });

    result.unwrap_or(STATUS_PANIC)
}

/// Get element from array by index.
#[no_mangle]
pub extern "C" fn jsonb_get_by_index(
    in_ptr: *const u8,
    in_len: usize,
    index: i32,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if in_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
            return STATUS_INVALID_ARG;
        }

        let input = unsafe { slice::from_raw_parts(in_ptr, in_len) };

        if input.is_empty() || input[0] != FORMAT_VERSION {
            return STATUS_INVALID_INPUT;
        }

        let jsonb_value = match jsonb::from_slice(&input[1..]) {
            Ok(v) => v,
            Err(_) => return STATUS_INVALID_INPUT,
        };

        let array = match jsonb_value.as_array() {
            Some(a) => a,
            None => return STATUS_INVALID_INPUT,
        };

        if index < 0 || index as usize >= array.len() {
            return STATUS_NOT_FOUND;
        }

        let result_value = &array[index as usize];
        let binary = result_value.to_vec();
        let mut result_bytes = Vec::with_capacity(1 + binary.len());
        result_bytes.push(FORMAT_VERSION);
        result_bytes.extend_from_slice(&binary);

        let boxed = result_bytes.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;

        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }

        STATUS_OK
    });

    result.unwrap_or(STATUS_PANIC)
}

/// Check if a contains b.
#[no_mangle]
pub extern "C" fn jsonb_contains(
    a_ptr: *const u8,
    a_len: usize,
    b_ptr: *const u8,
    b_len: usize,
    out_result: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if a_ptr.is_null() || b_ptr.is_null() || out_result.is_null() {
            return STATUS_INVALID_ARG;
        }

        let a_input = unsafe { slice::from_raw_parts(a_ptr, a_len) };
        let b_input = unsafe { slice::from_raw_parts(b_ptr, b_len) };

        if a_input.is_empty() || a_input[0] != FORMAT_VERSION ||
           b_input.is_empty() || b_input[0] != FORMAT_VERSION {
            return STATUS_INVALID_INPUT;
        }

        // Validate both inputs before calling contains
        match jsonb::from_slice(&a_input[1..]) {
            Ok(_) => {},
            Err(_) => return STATUS_INVALID_INPUT,
        };
        match jsonb::from_slice(&b_input[1..]) {
            Ok(_) => {},
            Err(_) => return STATUS_INVALID_INPUT,
        };

        // contains() expects raw binary slices without format version
        let contains = jsonb::contains(&a_input[1..], &b_input[1..]);
        unsafe {
            *out_result = if contains { 1 } else { 0 };
        }

        STATUS_OK
    });

    result.unwrap_or(STATUS_PANIC)
}

/// Check if key exists in object.
#[no_mangle]
pub extern "C" fn jsonb_exists_key(
    in_ptr: *const u8,
    in_len: usize,
    key_ptr: *const u8,
    key_len: usize,
    out_result: *mut u8,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if in_ptr.is_null() || key_ptr.is_null() || out_result.is_null() {
            return STATUS_INVALID_ARG;
        }

        let input = unsafe { slice::from_raw_parts(in_ptr, in_len) };
        let key_bytes = unsafe { slice::from_raw_parts(key_ptr, key_len) };

        if input.is_empty() || input[0] != FORMAT_VERSION {
            return STATUS_INVALID_INPUT;
        }

        let key = match std::str::from_utf8(key_bytes) {
            Ok(s) => s,
            Err(_) => return STATUS_INVALID_INPUT,
        };

        let jsonb_value = match jsonb::from_slice(&input[1..]) {
            Ok(v) => v,
            Err(_) => return STATUS_INVALID_INPUT,
        };

        let object = match jsonb_value.as_object() {
            Some(obj) => obj,
            None => return STATUS_INVALID_INPUT,
        };

        let exists = object.contains_key(key);
        unsafe {
            *out_result = if exists { 1 } else { 0 };
        }

        STATUS_OK
    });

    result.unwrap_or(STATUS_PANIC)
}

/// Get JSONB type name.
#[no_mangle]
pub extern "C" fn jsonb_type_of(
    in_ptr: *const u8,
    in_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if in_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
            return STATUS_INVALID_ARG;
        }

        let input = unsafe { slice::from_raw_parts(in_ptr, in_len) };

        if input.is_empty() || input[0] != FORMAT_VERSION {
            return STATUS_INVALID_INPUT;
        }

        let jsonb_value = match jsonb::from_slice(&input[1..]) {
            Ok(v) => v,
            Err(_) => return STATUS_INVALID_INPUT,
        };

        let type_name = match jsonb_value {
            jsonb::Value::Null => "null",
            jsonb::Value::Bool(_) => "boolean",
            jsonb::Value::Number(_) => "number",
            jsonb::Value::String(_) => "string",
            jsonb::Value::Array(_) => "array",
            jsonb::Value::Object(_) => "object",
        };

        let text_bytes = type_name.as_bytes().to_vec();
        let boxed = text_bytes.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;

        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }

        STATUS_OK
    });

    result.unwrap_or(STATUS_PANIC)
}

/// Get array length.
#[no_mangle]
pub extern "C" fn jsonb_array_length(
    in_ptr: *const u8,
    in_len: usize,
    out_length: *mut i64,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if in_ptr.is_null() || out_length.is_null() {
            return STATUS_INVALID_ARG;
        }

        let input = unsafe { slice::from_raw_parts(in_ptr, in_len) };

        if input.is_empty() || input[0] != FORMAT_VERSION {
            return STATUS_INVALID_INPUT;
        }

        let jsonb_value = match jsonb::from_slice(&input[1..]) {
            Ok(v) => v,
            Err(_) => return STATUS_INVALID_INPUT,
        };

        let length = match jsonb_value.array_length() {
            Some(len) => len as i64,
            None => return STATUS_INVALID_INPUT,
        };

        unsafe {
            *out_length = length;
        }

        STATUS_OK
    });

    result.unwrap_or(STATUS_PANIC)
}

/// Get object keys as JSONB array.
#[no_mangle]
pub extern "C" fn jsonb_object_keys(
    in_ptr: *const u8,
    in_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if in_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
            return STATUS_INVALID_ARG;
        }

        let input = unsafe { slice::from_raw_parts(in_ptr, in_len) };

        if input.is_empty() || input[0] != FORMAT_VERSION {
            return STATUS_INVALID_INPUT;
        }

        let jsonb_value = match jsonb::from_slice(&input[1..]) {
            Ok(v) => v,
            Err(_) => return STATUS_INVALID_INPUT,
        };

        let keys_value = match jsonb_value.object_keys() {
            Some(v) => v,
            None => return STATUS_INVALID_INPUT,
        };

        let binary = keys_value.to_vec();
        let mut result_bytes = Vec::with_capacity(1 + binary.len());
        result_bytes.push(FORMAT_VERSION);
        result_bytes.extend_from_slice(&binary);

        let boxed = result_bytes.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;

        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }

        STATUS_OK
    });

    result.unwrap_or(STATUS_PANIC)
}
