//! Define JNI APIs which can be called from Java/Scala.

use jni::objects::{JClass, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;

use crate::expression::{ArrayValues, ColumnarValue, Expr, LiteralValue, ArrayAccessor};
use crate::functions::BuiltinScalarFunction;
use crate::operators::{Execution, Operator};

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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
