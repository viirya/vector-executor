//! Define JNI APIs which can be called from Java/Scala.

use jni::objects::{JClass, JString, ReleaseMode};
use jni::sys::{jbyteArray, jint, jlong, jlongArray};
use jni::JNIEnv;

use arrow::ffi::ArrowArray;

use crate::expression::{ArrayAccessor, ArrayValues, ColumnarValue, Expr, LiteralValue};
use crate::functions::BuiltinScalarFunction;
use crate::operators::{Execution, Operator};
use crate::serde;
use crate::serde::Serde;

#[no_mangle]
/// Test JNI API. Accept a Java string and return it as an integer.
pub extern "system" fn Java_org_viirya_vector_native_VectorLib_test(
    env: JNIEnv,
    _class: JClass,
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
/// Test JNI API. Accept an address of OffHeapColumnVector of Spark, access its value and return.
pub extern "system" fn Java_org_viirya_vector_native_VectorLib_passOffHeapVector(
    _env: JNIEnv,
    _class: JClass,
    address: jlong,
    _num_row: jint,
) -> jint {
    let output = unsafe { *(address as *mut i32) };
    output
}

#[no_mangle]
/// Test JNI API. Accept an address of OffHeapColumnVector of Spark, an integer, then perform vectorized
/// add on the vector and the integer by a projection operator.
pub extern "system" fn Java_org_viirya_vector_native_VectorLib_projectOnVector(
    _env: JNIEnv,
    _class: JClass,
    address: jlong,
    num_row: jint,
    integer: jint,
) -> jint {
    let column_vector = ColumnarValue::Array(ArrayValues::IntColumnVector(address, num_row as u32));
    let scan = Operator::Scan(vec![column_vector]);

    let add = Expr::ScalarFunction {
        func: BuiltinScalarFunction::Add,
        args: vec![
            Expr::BoundReference(0),
            Expr::Literal(ColumnarValue::Scalar(LiteralValue::Int32(integer))),
        ],
    };

    let exprs = vec![add];
    let projection = Operator::Projection(exprs, Box::new(scan));
    let results = projection.execute().unwrap();

    results[0].get_int(0).unwrap() as i32
}

#[no_mangle]
/// Same as `Java_org_viirya_vector_native_VectorLib_projectOnTwoVectors`, but linked to different
/// Scala/JVM package.
pub extern "system" fn Java_org_apache_spark_sql_execution_NativeLibrary_projectOnTwoVectors(
    env: JNIEnv,
    _class: JClass,
    address1: jlong,
    address2: jlong,
    num_row: jint,
) -> jlongArray {
    project_on_two_vectors(env, _class, address1, address2, num_row)
}

#[no_mangle]
/// Test JNI API. Accept the addresses of two OffHeapColumnVectors of Spark, then perform vectorized
/// add on the two vectors by a projection operator. Return address of arrow vector.
pub extern "system" fn Java_org_viirya_vector_native_VectorLib_projectOnTwoVectors(
    env: JNIEnv,
    _class: JClass,
    address1: jlong,
    address2: jlong,
    num_row: jint,
) -> jlongArray {
    project_on_two_vectors(env, _class, address1, address2, num_row)
}

fn project_on_two_vectors(
    env: JNIEnv,
    _class: JClass,
    address1: jlong,
    address2: jlong,
    num_row: jint,
) -> jlongArray {
    let column_vector1 =
        ColumnarValue::Array(ArrayValues::IntColumnVector(address1, num_row as u32));
    let column_vector2 =
        ColumnarValue::Array(ArrayValues::IntColumnVector(address2, num_row as u32));

    let scan = Operator::Scan(vec![column_vector1, column_vector2]);

    let add = Expr::ScalarFunction {
        func: BuiltinScalarFunction::Add,
        args: vec![Expr::BoundReference(0), Expr::BoundReference(1)],
    };

    let exprs = vec![add];
    let projection = Operator::Projection(exprs, Box::new(scan));
    let results = projection.execute().unwrap();

    match results.get(0).unwrap() {
        ColumnarValue::Array(ArrayValues::ArrowArray(array_ref)) => {
            let (array, schema) = unsafe {
                ArrowArray::into_raw(ArrowArray::try_new(array_ref.data().clone()).unwrap())
            };

            let long_array = env.new_long_array(2).unwrap();
            env.set_long_array_region(long_array, 0, &vec![array as i64, schema as i64])
                .unwrap();

            return long_array;
        }
        _ => return env.new_long_array(0).unwrap(),
    }
}

#[no_mangle]
/// Test JNI API. Accept serialized query plan and the addresses of OffHeapColumnVectors of Spark,
/// then execute the query. Return addresses of arrow vector.
pub extern "system" fn Java_org_viirya_vector_native_VectorLib_executePlan(
    env: JNIEnv,
    _class: JClass,
    serialized_query: jbyteArray,
    addresses: jlongArray,
    num_row: jint,
) -> jlongArray {
    let num_arrays = env.get_array_length(addresses).unwrap() as usize;
    let array_elements = env
        .get_long_array_elements(addresses, ReleaseMode::NoCopyBack)
        .unwrap()
        .as_ptr();

    let mut i: usize = 0;
    let mut inputs: Vec<ColumnarValue> = vec![];
    while i < num_arrays {
        let array = ColumnarValue::Array(ArrayValues::IntColumnVector(
            unsafe { *(array_elements.add(i)) },
            num_row as u32,
        ));
        inputs.push(array);
        i += 1;
    }

    let bytes_elements = env
        .get_byte_array_elements(serialized_query, ReleaseMode::NoCopyBack)
        .unwrap()
        .as_ptr();
    let mut encoded: Vec<u8> = vec![];
    i = 0;
    while i < env.get_array_length(serialized_query).unwrap() as usize {
        let byte = unsafe { *(bytes_elements.add(i)) };
        encoded.push(byte as u8);
        i += 1;
    }

    // Deserialize query plan
    let query = serde::deserialize_op(&encoded.as_slice())
        .unwrap()
        .to_native()
        .unwrap();

    // Replace with inputs
    let query_with_inputs = match query {
        Operator::Projection(exprs, child) => {
            let new_child = match *child {
                Operator::Scan(arrays) if arrays.len() == 0 => Operator::Scan(inputs),
                _ => *child,
            };
            Operator::Projection(exprs, Box::new(new_child))
        }
        _ => query,
    };

    let results = query_with_inputs.execute().unwrap();

    match results.get(0).unwrap() {
        ColumnarValue::Array(ArrayValues::ArrowArray(array_ref)) => {
            let (array, schema) = unsafe {
                ArrowArray::into_raw(ArrowArray::try_new(array_ref.data().clone()).unwrap())
            };

            let long_array = env.new_long_array(2).unwrap();
            env.set_long_array_region(long_array, 0, &vec![array as i64, schema as i64])
                .unwrap();

            return long_array;
        }
        _ => return env.new_long_array(0).unwrap(),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
