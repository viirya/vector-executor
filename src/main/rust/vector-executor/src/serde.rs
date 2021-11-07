//! Ser/De for expression/operators.

use std::io::Cursor;
use prost::Message;

use crate::spark_expression;

pub fn serialize_expr(expr: &spark_expression::Expr) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.reserve(expr.encoded_len());
    // Unwrap is safe, since we have reserved sufficient capacity in the vector.
    expr.encode(&mut buf).unwrap();
    buf
}

pub fn deserialize_expr(buf: &[u8]) -> Result<spark_expression::Expr, prost::DecodeError> {
    spark_expression::Expr::decode(&mut Cursor::new(buf))
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
