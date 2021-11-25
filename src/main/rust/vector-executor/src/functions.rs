//! Functions.

use super::expression::{ArrayAccessor, ArrayValues, ColumnarValue, Expr, ExpressionError};

use arrow::array::Int32Array;

use std::iter;
use std::sync::Arc;

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
    let mut idx = 0;
    let values = iter::repeat_with(|| {
        let sum = args[0]
            .get_int(idx)
            .unwrap()
            .checked_add(args[1].get_int(idx).unwrap())
            .unwrap_or(i32::MAX);
        idx += 1;
        sum
    })
    .take(len);
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

    #[test]
    fn test_build_slice_from_column_vector() {
        let vector = vec![1, 2, 3];
        let array = ArrayValues::IntColumnVector(vector.as_ptr() as i64, 3 as u32);
        let slice = int_col_vector(&array);
        assert_eq!(slice.len(), 3);
        assert_eq!(slice, vector);
    }
}
