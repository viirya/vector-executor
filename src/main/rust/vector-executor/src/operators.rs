//! Operators

use super::expression::ColumnarValue;
use super::expression::{Expr, ExpressionError};
use super::functions;

use std::boxed::Box;

/// Error returned during executing operators.
#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    /// Simple error
    #[allow(dead_code)]
    #[error("General execution error with reason {0}.")]
    GeneralError(String),

    /// Error returned when reading the delta log object failed.
    #[error("Failed to execute expression: {}", .source)]
    ExpressionError {
        /// Expression error details when executing the operator.
        #[from]
        source: ExpressionError,
    },
}

/// An vectorization operator
#[derive(Clone)]
pub enum Operator {
    /// Scan
    Scan(Vec<ColumnarValue>),
    /// Projection
    Projection(Vec<Expr>, Box<Operator>),
}

trait Execution {
    fn execute(&self) -> Result<Vec<ColumnarValue>, ExecutionError>;
}

impl Execution for Operator {
    fn execute(&self) -> Result<Vec<ColumnarValue>, ExecutionError> {
        match self {
            Operator::Scan(vectors) => Ok(vectors.clone()),
            Operator::Projection(exprs, child) => {
                let child_executed = child.as_ref().execute().unwrap();

                let results = exprs
                    .iter()
                    .map(|expr| execute_expr(expr, child_executed.as_slice()))
                    .into_iter()
                    .collect::<Vec<_>>();

                let mut vectors: Vec<ColumnarValue> = vec![];

                for r in results {
                    match r {
                        Err(e) => return Err(ExecutionError::from(e)),
                        Ok(c) => vectors.push(c),
                    }
                }

                Ok(vectors)
            }
        }
    }
}

fn execute_expr(expr: &Expr, args: &[ColumnarValue]) -> Result<ColumnarValue, ExpressionError> {
    match expr {
        Expr::Literal(lit) => Ok(lit.clone()),

        Expr::ScalarFunction {
            func: expr_func,
            args: expr_args,
        } => {
            let children = expr_args
                .into_iter()
                .map(|expr| execute_expr(expr, args))
                .into_iter()
                .collect::<Vec<_>>();
            let mut vectors: Vec<ColumnarValue> = vec![];

            for r in children {
                match r {
                    Err(e) => return Err(e),
                    Ok(c) => vectors.push(c),
                }
            }
            execute_scalar_fun(expr_func, &vectors)
        }
    }
}

fn execute_scalar_fun(
    func: &functions::BuiltinScalarFunction,
    args: &[ColumnarValue],
) -> Result<ColumnarValue, ExpressionError> {
    match func {
        functions::BuiltinScalarFunction::Add => functions::physical_add(args),
    }
}

#[cfg(test)]
mod tests {
    use crate::expression::{ArrayValues, ColumnarValue, Expr, LiteralValue};
    use crate::functions::BuiltinScalarFunction;
    use crate::operators::{Execution, Operator};

    use arrow::array::Int32Array;
    use std::sync::Arc;
    use arrow::datatypes::DataType::Int32;

    #[test]
    fn test_projection() {
        let array1 = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let array2 = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let array3 = Int32Array::from(vec![1, 2, 3, 4, 5]);

        let arrow_array1 = ColumnarValue::Array(ArrayValues::ArrowArray(Arc::new(array1)));
        let arrow_array2 = ColumnarValue::Array(ArrayValues::ArrowArray(Arc::new(array2)));

        let scan = Operator::Scan(vec![arrow_array1, arrow_array2]);

        let add1 = Expr::ScalarFunction {
            func: BuiltinScalarFunction::Add,
            args: vec![
                Expr::Literal(ColumnarValue::Scalar(LiteralValue::Int32(1))),
                Expr::Literal(ColumnarValue::Array(ArrayValues::ArrowArray(Arc::new(array3)))),
            ],
        };
        let exprs = vec![add1];
        let projection = Operator::Projection(exprs, Box::new(scan));
        let results = projection.execute().unwrap();

        assert_eq!(results.len(), 1);

        match results.get(0).unwrap() {
            ColumnarValue::Array(ArrayValues::ArrowArray(array_ref)) => {
                assert_eq!(array_ref.len(), 5);
                assert_eq!(array_ref.data().data_type().clone(), Int32);
                assert_eq!(array_ref.as_ref(), &Int32Array::from(vec![2, 3, 4, 5, 6]));
            },
            _ => assert!(false, "Add expression should return ArrowArray"),
        }
    }
}
