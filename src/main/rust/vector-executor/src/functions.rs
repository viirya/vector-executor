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
        #[allow(unreachable_patterns)]
        _ => {
            return Err(ExpressionError::GeneralError(
                "Expect Add function to take two array params".to_string(),
            ))
        }
    };
    let mut idx = 0;
    let values = iter::repeat_with(|| {
        let sum = args[0].get_int(idx).unwrap() + args[1].get_int(idx).unwrap();
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
