//! Ser/De for expression/operators.

use prost::Message;
use std::io::Cursor;
// use std::fmt::{Debug, Formatter};

use crate::expression;
use crate::functions;
use crate::spark_expression;

impl From<prost::DecodeError> for expression::ExpressionError {
    fn from(error: prost::DecodeError) -> expression::ExpressionError {
        expression::ExpressionError::DeserializeError(error.to_string())
    }
}

/*
impl Debug for spark_expression::expr::Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            spark_expression::expr::Expr::Add(..) => f.write_str("add"),
            spark_expression::expr::Expr::Literal(..) => f.write_str("literal"),
        }
    }
}
 */

/// Trait for serialization/deserialization
/// E is error type, N is native type
pub trait Serde<E, N>
where
    Self: Sized,
{
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
            Some(spark_expression::expr::ExprType::Add) => match self.expr_struct.as_ref().unwrap()
            {
                spark_expression::expr::ExprStruct::Add(add) => {
                    let left = add.left.as_ref().unwrap().convert_to_native().unwrap();
                    let right = add.right.as_ref().unwrap().convert_to_native().unwrap();

                    Ok(functions::add(left.clone(), right.clone()))
                }
                other => Err(expression::ExpressionError::GeneralError(format!(
                    "Add message type shouldn't have {:?} expr!",
                    other
                ))),
            },
            Some(spark_expression::expr::ExprType::Literal) => {
                match self.expr_struct.as_ref().unwrap() {
                    spark_expression::expr::ExprStruct::Literal(spark_expression::Literal {
                        value,
                    }) => match value {
                        Some(spark_expression::literal::Value::IntVal(i)) => {
                            Ok(expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Int32(*i),
                            )))
                        }
                        Some(spark_expression::literal::Value::LongVal(l)) => {
                            Ok(expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Int64(*l),
                            )))
                        }
                        Some(spark_expression::literal::Value::FloatVal(f)) => {
                            Ok(expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Float(*f),
                            )))
                        }
                        Some(spark_expression::literal::Value::DoubleVal(d)) => {
                            Ok(expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Double(*d),
                            )))
                        }
                        Some(spark_expression::literal::Value::StringVal(s)) => {
                            Ok(expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::String(s.clone()),
                            )))
                        }
                        Some(spark_expression::literal::Value::BytesVal(b)) => {
                            Ok(expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Bytes(b.clone()),
                            )))
                        }
                        Some(spark_expression::literal::Value::BoolVal(b)) => {
                            Ok(expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Bool(*b),
                            )))
                        },
                        None => Err(expression::ExpressionError::GeneralError(format!(
                            "Literal message type shouldn't have empty value!"))),
                    },
                    other => Err(expression::ExpressionError::GeneralError(format!(
                        "Literal message type shouldn't have {:?} expr!",
                        other
                    ))),
                }
            }
            /*
            Some(_) => Err(expression::ExpressionError::NativeExprNotFound(
                self.expr_type as i32,
            )),
             */
            None => Err(expression::ExpressionError::NativeExprNotFound(
                self.expr_type as i32,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::expression;
    use crate::functions;
    use crate::serde::Serde;
    use crate::spark_expression;

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

    #[test]
    fn to_native() {
        let mut expr = spark_expression::Expr::default();
        expr.set_expr_type(spark_expression::expr::ExprType::Add);

        let mut literal1 = spark_expression::Expr::default();
        literal1.set_expr_type(spark_expression::expr::ExprType::Literal);
        literal1.expr_struct = Some(spark_expression::expr::ExprStruct::Literal(
            spark_expression::Literal {
                value: Some(spark_expression::literal::Value::IntVal(1)),
            },
        ));
        let mut literal2 = spark_expression::Expr::default();
        literal2.set_expr_type(spark_expression::expr::ExprType::Literal);
        literal2.expr_struct = Some(spark_expression::expr::ExprStruct::Literal(
            spark_expression::Literal {
                value: Some(spark_expression::literal::Value::IntVal(1)),
            },
        ));

        expr.expr_struct = Some(spark_expression::expr::ExprStruct::Add(Box::new(
            spark_expression::Add {
                left: Some(Box::new(literal1)),
                right: Some(Box::new(literal2)),
            },
        )));

        let encoded = expr.serialize();
        match expr.to_native(&encoded.as_slice()).unwrap() {
            expression::Expr::ScalarFunction { func, .. } => {
                assert_eq!(func, functions::BuiltinScalarFunction::Add)
            }
            _ => assert!(false, "wrong native expression!"),
        }
    }
}
