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

/// Trait for serialization/deserialization
/// E is error type, N is native type
pub trait Serde<E, N> where Self: Sized {
    /// Serialize intermediate object to bytes
    fn serialize(self: &Self) -> Vec<u8>;

    /// Deserilize bytes to natintermediateive object
    fn deserialize(self: &Self, buf: &[u8]) -> Result<Self, E>;

    /// Deserilize bytes to native object, e.g. expression, operator
    fn to_native(self: &Self, buf: &[u8]) -> Result<N, E>;

    /// Convert from intermediate object ot native object
    fn convert_to_native(self: &Self) -> Result<N, E>;
}

impl Serde<expression::ExpressionError, expression::Expr> for spark_expression::Expr {
    #[allow(dead_code)]
    fn serialize(self: &Self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.reserve(self.encoded_len());
        // Unwrap is safe, since we have reserved sufficient capacity in the vector.
        self.encode(&mut buf).unwrap();
        buf
    }

    fn deserialize(self: &Self, buf: &[u8]) -> Result<Self, expression::ExpressionError> {
        match spark_expression::Expr::decode(&mut Cursor::new(buf)) {
            Ok(e) => Ok(e),
            Err(err) => Err(expression::ExpressionError::from(err)),
        }
    }

    fn to_native(self: &Self, buf: &[u8]) -> Result<expression::Expr, expression::ExpressionError> {
        let decoded = self.deserialize(buf)?;
        decoded.convert_to_native()
    }

    fn convert_to_native(self: &Self) -> Result<expression::Expr, expression::ExpressionError> {
        match spark_expression::expr::ExprType::from_i32(self.expr_type) {
            Some(spark_expression::expr::ExprType::Add) => {
                let children = self.children.iter().map(|e| {
                    e.convert_to_native().unwrap()
                }).into_iter().collect::<Vec<expression::Expr>>();

                Ok(functions::add( children.get(0).unwrap().clone(), children.get(0).unwrap().clone()))
            },
            None => Err(expression::ExpressionError::NativeExprNotFound(self.expr_type as i32)),
        }
    }
}




#[cfg(test)]
mod tests {
    use crate::spark_expression;
    use crate::serde::Serde;

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

        let encoded = expr.serialize();
        let decoded = expr.deserialize(&encoded.as_slice());

        assert_eq!(expr, decoded.unwrap())
    }
}
