//! Expressions.

// use super::datatype::DataType;
use super::functions::BuiltinScalarFunction;

// use serde::{Deserialize, Serialize};
use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Binary, Boolean, Float32, Float64, Int32, Int64, Utf8};

/// Error returned when there is an error during executing an expression.
#[derive(thiserror::Error, Debug)]
pub enum ExpressionError {
    /// Simple error
    #[allow(dead_code)]
    #[error("General expression error with reason {0}.")]
    GeneralError(String),

    /// Cannot find corresponding native expression
    #[error("Fail to find native expression from serialized expr {0},")]
    NativeExprNotFound(i32),

    /// Deserializarion error
    #[error("Fail to deserialize to native expression with reason {0},")]
    DeserializeError(String),
}

/// An vectorization expression
#[derive(Clone, Debug)]
pub enum Expr {
    /// Literal
    Literal(ColumnarValue),

    /// BoundReference
    BoundReference(usize),

    /// Scala functions for expression
    ScalarFunction {
        /// The function
        func: BuiltinScalarFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
    },
}

/// Literal value
#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub enum LiteralValue {
    /// Int value
    Int32(i32),
    /// Long value
    Int64(i64),
    /// Float value
    Float(f32),
    /// Double value
    Double(f64),
    /// String value
    String(String),
    /// Binary value
    Bytes(Vec<u8>),
    /// Boolean value
    Bool(bool),
}

/// Represents an array of values
#[derive(Clone, Debug)]
pub enum ArrayValues {
    /// Arrow Array of values
    ArrowArray(ArrayRef),
    /// Spark Integer ColumnVector of values (address and number of rows)
    #[allow(dead_code)]
    IntColumnVector(i64, u32),
}

impl ArrayValues {
    /// Return length of this array
    pub fn len(&self) -> u32 {
        match self {
            ArrayValues::ArrowArray(array_ref) => array_ref.len() as u32,
            ArrayValues::IntColumnVector(_, len) => len.clone(),
        }
    }
}

/// Represents the result from an expression
#[derive(Clone, Debug)]
pub enum ColumnarValue {
    /// Array of values
    Array(ArrayValues),
    /// Singular value
    Scalar(LiteralValue),
}

/// Accessor of an array
pub trait ArrayAccessor {
    /// Return integer at specified index
    fn get_int(&self, index: u32) -> Result<i32, ExpressionError>;
}

impl ColumnarValue {
    #[allow(dead_code)]
    fn data_type(&self) -> DataType {
        match self {
            ColumnarValue::Scalar(LiteralValue::Int32(_)) => Int32,
            ColumnarValue::Scalar(LiteralValue::Int64(_)) => Int64,
            ColumnarValue::Scalar(LiteralValue::Float(_)) => Float32,
            ColumnarValue::Scalar(LiteralValue::Double(_)) => Float64,
            ColumnarValue::Scalar(LiteralValue::String(_)) => Utf8,
            ColumnarValue::Scalar(LiteralValue::Bytes(_)) => Binary,
            ColumnarValue::Scalar(LiteralValue::Bool(_)) => Boolean,
            ColumnarValue::Array(ArrayValues::ArrowArray(array_value)) => {
                array_value.data_type().clone()
            }
            ColumnarValue::Array(ArrayValues::IntColumnVector(_, _)) => Int32,
        }
    }

    /// Return length of this array
    pub fn len(&self) -> u32 {
        match self {
            ColumnarValue::Scalar(_) => 1,
            ColumnarValue::Array(ArrayValues::ArrowArray(array_value)) => array_value.len() as u32,
            ColumnarValue::Array(ArrayValues::IntColumnVector(_, len)) => len.clone(),
        }
    }
}

impl ArrayAccessor for ColumnarValue {
    fn get_int(&self, index: u32) -> Result<i32, ExpressionError> {
        match self {
            ColumnarValue::Scalar(LiteralValue::Int32(i)) => Ok(i.clone()),
            // As Spark already analyzes and resolves the expression, we should not ask int from other scalar literals.
            ColumnarValue::Scalar(literal) => Err(ExpressionError::GeneralError(format!(
                "Error when getting integer from a scalar literal {:?}",
                literal
            ))),
            ColumnarValue::Array(ArrayValues::ArrowArray(array_value)) => Ok(array_value
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(index.try_into().unwrap())),
            ColumnarValue::Array(ArrayValues::IntColumnVector(address, rows)) => {
                if index >= *rows {
                    return Err(ExpressionError::GeneralError(format!(
                        "index {} is out for array range",
                        index
                    )));
                } else {
                    Ok(unsafe {
                        *((address + (index * std::mem::size_of::<i32>() as u32) as i64)
                            as *mut i32)
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::expression::{ArrayValues, ColumnarValue};

    use arrow::array::Int32Array;
    use arrow::datatypes::DataType::Int32;
    use std::sync::Arc;

    #[test]
    fn arrow_array_values() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        assert_eq!(array.len(), 5);

        let arrow_array = ColumnarValue::Array(ArrayValues::ArrowArray(Arc::new(array)));

        assert_eq!(arrow_array.len(), 5);
        assert_eq!(arrow_array.data_type(), Int32);
    }
}
