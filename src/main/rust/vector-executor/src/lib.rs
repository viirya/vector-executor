use jni::objects::{JClass, JString, JValue};
use jni::sys::{jint, jlong, jlongArray, jstring};
use jni::JNIEnv;

use std::boxed::Box;
use std::env;
use std::fs::File;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;
use std::vec::Vec;

#[no_mangle]
pub extern "system" fn Java_org_viirya_vector_native_VectorLib_test(
    env: JNIEnv,
    class: JClass,
    input: JString,
) -> jint {
    let input: String = env
        .get_string(input)
        .expect("Couldn't get java string!")
        .into();

    let output = input.parse::<i32>().unwrap();

    output
}

#[no_mangle]
pub extern "system" fn Java_org_viirya_vector_native_VectorLib_passOffHeapVector(
    env: JNIEnv,
    class: JClass,
    address: jlong,
    num_row: jint,
) -> jint {
    /*
    let raw_address: *mut [i32] = address as *mut [i32];

    let values = unsafe {
        Box::<[i32]>::from_raw(raw_address)
    };
    values[0].count_zeros() as i64
     */

    let output = unsafe { *(address as *mut i32) };
    output
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
