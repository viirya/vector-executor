//! Ser/De for expression/operators.

use std::io::Cursor;
use prost::Message;

use crate::spark_expression;
use crate::expression;
use crate::functions;

impl From<prost::DecodeError> for expression::ExpressionError {
    fn from(error: prost::DecodeError) -> expression::ExpressionError {
        expression::ExpressionError::DeserializeError(error.to_string())
    }
}

#[allow(dead_code)]
fn serialize_expr(expr: &spark_expression::Expr) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.reserve(expr.encoded_len());
    // Unwrap is safe, since we have reserved sufficient capacity in the vector.
    expr.encode(&mut buf).unwrap();
    buf
}

fn deserialize_expr(buf: &[u8]) -> Result<spark_expression::Expr, expression::ExpressionError> {
    match spark_expression::Expr::decode(&mut Cursor::new(buf)) {
        Ok(e) => Ok(e),
        Err(err) => Err(expression::ExpressionError::from(err)),
    }
}

/// Deserilize bytes to native expression
pub fn deserialize(buf: &[u8]) -> Result<expression::Expr, expression::ExpressionError> {
    let decoded = deserialize_expr(buf)?;
    convert_to_native_expr(&decoded)
}

fn convert_to_native_expr(expr: &spark_expression::Expr) -> Result<expression::Expr, expression::ExpressionError> {
    match spark_expression::expr::ExprType::from_i32(expr.expr_type) {
        Some(spark_expression::expr::ExprType::Add) => {
            let children = expr.children.iter().map(|e| {
                convert_to_native_expr(&e).unwrap()
            }).into_iter().collect::<Vec<expression::Expr>>();

            Ok(functions::add( children.get(0).unwrap().clone(), children.get(0).unwrap().clone()))
        },
        None => Err(expression::ExpressionError::NativeExprNotFound(expr.expr_type as i32)),
    }
}


#[cfg(test)]
mod tests {
    use crate::spark_expression;
    use crate::serde::{serialize_expr, deserialize_expr};

    #[test]
    fn basic() {
        let mut default_expr = spark_expression::Expr::default();
        default_expr.set_expr_type(spark_expression::expr::ExprType::Add);
        assert_eq!(
            default_expr.expr_type,
            spark_expression::expr::ExprType::Add as i32
        );
    }

    #[test]
    fn ser_de() {
        let mut expr = spark_expression::Expr::default();
        expr.set_expr_type(spark_expression::expr::ExprType::Add);

        let encoded = serialize_expr(&expr);
        let decoded = deserialize_expr(&encoded.as_slice());

        assert_eq!(expr, decoded.unwrap())
    }
}
