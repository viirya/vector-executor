//! Functions.

use super::expression::{ArrayAccessor, ArrayValues, ColumnarValue, Expr, ExpressionError};

use arrow::array::Int32Array;
use std::iter;
use std::sync::Arc;

use simdeez::avx2::*;
use simdeez::scalar::*;
use simdeez::sse2::*;
use simdeez::sse41::*;

/// Return a sum of left and right expressions
#[allow(dead_code)]
pub fn add(left: Expr, right: Expr) -> Expr {
    Expr::ScalarFunction {
        func: BuiltinScalarFunction::Add,
        args: vec![left, right],
    }
}

macro_rules! build_slice_from_column_vector {
    ($name:ident, $col_vec_type: ident, $element_type: ty) => {
        #[allow(dead_code)]
        /// Build slice from given column vector
        pub fn $name<'a>(col: &'a ArrayValues) -> &'a [$element_type] {
            match col {
                ArrayValues::$col_vec_type(address, num_row) => {
                    let raw_ptr = *address as *mut $element_type;
                    unsafe { ::std::slice::from_raw_parts(raw_ptr, *num_row as usize) }
                }
                _ => unreachable!(),
            }
        }
    };
}

build_slice_from_column_vector!(int_col_vector, IntColumnVector, i32);

simd_runtime_generate!(
    fn add_vectors(x: &[i32], y: &[i32]) -> Vec<i32> {
        let mut result: Vec<i32> = Vec::with_capacity(x.len());
        result.set_len(x.len());

        let mut x1 = &x[..x.len()];
        let mut y1 = &y[..y.len()];
        let mut res = &mut result[..x.len()];

        while x1.len() >= S::VI32_WIDTH {
            let xv = S::loadu_epi32(&x1[0]);
            let yv = S::loadu_epi32(&y1[0]);

            let r = xv + yv;
            S::storeu_epi32(&mut res[0], r);

            x1 = &x1[S::VI32_WIDTH..];
            y1 = &y1[S::VI32_WIDTH..];
            res = &mut res[S::VI32_WIDTH..];
        }

        for i in 0..x1.len() {
            res[i] = x1[i] + y1[i];
        }

        result
    }
);

/// Physical Add function
#[allow(dead_code)]
pub fn physical_add(args: &[ColumnarValue]) -> Result<ColumnarValue, ExpressionError> {
    let len: usize = match (&args[0], &args[1]) {
        (ColumnarValue::Array(array1), ColumnarValue::Array(array2)) => {
            if array1.len() == array2.len() {
                array1.len().try_into().unwrap()
            } else {
                return Err(ExpressionError::GeneralError(
                    "The params of Add expr should have same lengths".to_string(),
                ));
            }
        }
        (ColumnarValue::Array(array), ColumnarValue::Scalar(_)) => array.len().try_into().unwrap(),
        (ColumnarValue::Scalar(_), ColumnarValue::Array(array)) => array.len().try_into().unwrap(),
        #[allow(unreachable_patterns)]
        _ => {
            return Err(ExpressionError::GeneralError(
                "Expect Add function to take at lease one array param".to_string(),
            ))
        }
    };

    let values = match (&args[0], &args[1]) {
        (ColumnarValue::Array(array1), ColumnarValue::Array(array2)) => match (array1, array2) {
            (ArrayValues::IntColumnVector(_, _), ArrayValues::IntColumnVector(_, _)) => {
                // Use SIMD
                let slice1 = int_col_vector(array1);
                let slice2 = int_col_vector(array2);

                unsafe {
                    if is_x86_feature_detected!("sse2") {
                        add_vectors_sse2(slice1, slice2)
                    } else if is_x86_feature_detected!("sse4.1") {
                        add_vectors_sse41(slice1, slice2)
                    } else if is_x86_feature_detected!("avx2") {
                        add_vectors_avx2(slice1, slice2)
                    } else {
                        let mut idx = 0;
                        let result = iter::repeat_with(|| {
                            let sum = args[0]
                                .get_int(idx)
                                .unwrap()
                                .checked_add(args[1].get_int(idx).unwrap())
                                .unwrap_or(i32::MAX);
                            idx += 1;
                            sum
                        })
                        .take(len)
                        .collect::<Vec<_>>();
                        result
                    }
                }
            }
            // todo: Use Arrow SIMD for Arrow vectors
            _ => {
                let mut idx = 0;
                let result = iter::repeat_with(|| {
                    let sum = args[0]
                        .get_int(idx)
                        .unwrap()
                        .checked_add(args[1].get_int(idx).unwrap())
                        .unwrap_or(i32::MAX);
                    idx += 1;
                    sum
                })
                .take(len)
                .collect::<Vec<_>>();
                result
            }
        },
        // todo: scalar + vector
        // (ColumnarValue::Scalar(_), ColumnarValue::Array(array)) => array.len().try_into().unwrap(),
        #[allow(unreachable_patterns)]
        _ => unreachable!(),
    };

    let array = Int32Array::from_iter_values(values);
    Ok(ColumnarValue::Array(ArrayValues::ArrowArray(Arc::new(
        array,
    ))))
}

/// Enum of all built-in scalar functions
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum BuiltinScalarFunction {
    /// Add
    Add,
}

#[cfg(test)]
mod tests {
    use super::int_col_vector;
    use crate::expression::ArrayValues;
    use crate::functions::*;

    #[test]
    fn test_build_slice_from_column_vector() {
        let vector = vec![1, 2, 3];
        let array = ArrayValues::IntColumnVector(vector.as_ptr() as i64, 3 as u32);
        let slice = int_col_vector(&array);
        assert_eq!(slice.len(), 3);
        assert_eq!(slice, vector);
    }

    #[test]
    fn test_add_two_vectors_by_simd() {
        let vector1 = vec![1, 2, 3, 4];
        let array1 = ArrayValues::IntColumnVector(vector1.as_ptr() as i64, 4 as u32);
        let slice1 = int_col_vector(&array1);

        let vector2 = vec![4, 5, 6, 7];
        let array2 = ArrayValues::IntColumnVector(vector2.as_ptr() as i64, 4 as u32);
        let slice2 = int_col_vector(&array2);

        let result = unsafe {
            if is_x86_feature_detected!("sse2") {
                add_vectors_sse2(slice1, slice2)
            } else if is_x86_feature_detected!("sse4.1") {
                add_vectors_sse41(slice1, slice2)
            } else if is_x86_feature_detected!("avx2") {
                add_vectors_avx2(slice1, slice2)
            } else {
                panic!("not support SIMD!");
            }
        };

        assert_eq!(result.len(), 4);
        assert_eq!(result, vec![5, 7, 9, 11]);
    }

    #[test]
    fn test_simd_physical_add() {
        let vector1 = vec![1, 2, 3];
        let array1 = ArrayValues::IntColumnVector(vector1.as_ptr() as i64, 3 as u32);

        let vector2 = vec![4, 5, 6];
        let array2 = ArrayValues::IntColumnVector(vector2.as_ptr() as i64, 3 as u32);

        let added_result = physical_add(
            vec![ColumnarValue::Array(array1), ColumnarValue::Array(array2)].as_slice(),
        )
        .expect("Fail to add two vectors");

        match added_result {
            ColumnarValue::Array(ArrayValues::ArrowArray(a)) => {
                assert_eq!(a.len(), 3);

                let array = Int32Array::from(vec![5, 7, 9]);
                assert_eq!(array.values(), vec![5, 7, 9].as_slice());
            }
            _ => panic!("should not reach here"),
        }
    }
}
