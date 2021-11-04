//! Operators

use super::expression::{Expr, ExpressionError};
use super::expression::ColumnarValue;
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

                let results = exprs.iter().map(|expr| {
                    execute_expr(expr, child_executed.as_slice())
                }).into_iter().collect::<Vec<_>>();

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
        Expr::ScalarFunction {func: expr_func, args: expr_args} => {
            let children = expr_args.into_iter().map(|expr| execute_expr(expr, args)).into_iter().collect::<Vec<_>>();
            let mut vectors: Vec<ColumnarValue> = vec![];

            for r in children {
                match r {
                    Err(e) => return Err(e),
                    Ok(c) => vectors.push(c),
                }
            }
            execute_scalar_fun(expr_func, &vectors)
        },
    }
}

fn execute_scalar_fun(func: &functions::BuiltinScalarFunction, args: &[ColumnarValue]) -> Result<ColumnarValue, ExpressionError> {
    match func {
        functions::BuiltinScalarFunction::Add => {
            functions::physical_add(args)
        },
    }
}
