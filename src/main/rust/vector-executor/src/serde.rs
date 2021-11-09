//! Ser/De for expression/operators.

use prost::Message;
use std::io::Cursor;

use crate::expression;
use crate::functions;
use crate::operators::{ExecutionError, Operator};
use crate::spark_expression;
use crate::spark_operator;

impl From<prost::DecodeError> for expression::ExpressionError {
    fn from(error: prost::DecodeError) -> expression::ExpressionError {
        expression::ExpressionError::DeserializeError(error.to_string())
    }
}

impl From<prost::DecodeError> for ExecutionError {
    fn from(error: prost::DecodeError) -> ExecutionError {
        ExecutionError::DeserializeError(error.to_string())
    }
}

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
                other => Err(expression::ExpressionError::DeserializeError(format!(
                    "Add message type shouldn't have {:?} expr!",
                    other
                ))),
            },
            Some(spark_expression::expr::ExprType::Literal) => {
                match self.expr_struct.as_ref().unwrap() {
                    spark_expression::expr::ExprStruct::Literal(spark_expression::Literal {
                        value,
                    }) => match value {
                        Some(spark_expression::literal::Value::IntVal(i)) => Ok(
                            expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Int32(*i),
                            )),
                        ),
                        Some(spark_expression::literal::Value::LongVal(l)) => Ok(
                            expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Int64(*l),
                            )),
                        ),
                        Some(spark_expression::literal::Value::FloatVal(f)) => Ok(
                            expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Float(*f),
                            )),
                        ),
                        Some(spark_expression::literal::Value::DoubleVal(d)) => Ok(
                            expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Double(*d),
                            )),
                        ),
                        Some(spark_expression::literal::Value::StringVal(s)) => Ok(
                            expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::String(s.clone()),
                            )),
                        ),
                        Some(spark_expression::literal::Value::BytesVal(b)) => Ok(
                            expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Bytes(b.clone()),
                            )),
                        ),
                        Some(spark_expression::literal::Value::BoolVal(b)) => Ok(
                            expression::Expr::Literal(expression::ColumnarValue::Scalar(
                                expression::LiteralValue::Bool(*b),
                            )),
                        ),
                        None => Err(expression::ExpressionError::GeneralError(format!(
                            "Literal message type shouldn't have empty value!"
                        ))),
                    },
                    other => Err(expression::ExpressionError::DeserializeError(format!(
                        "Literal message type shouldn't have {:?} expr!",
                        other
                    ))),
                }
            }
            None => Err(expression::ExpressionError::NativeExprNotFound(
                self.expr_type as i32,
            )),
        }
    }
}

impl Serde<ExecutionError, Operator> for spark_operator::Operator {
    fn serialize(self: &Self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.reserve(self.encoded_len());
        // Unwrap is safe, since we have reserved sufficient capacity in the vector.
        self.encode(&mut buf).unwrap();
        buf
    }

    fn deserialize(self: &Self, buf: &[u8]) -> Result<Self, ExecutionError> {
        match spark_operator::Operator::decode(&mut Cursor::new(buf)) {
            Ok(e) => Ok(e),
            Err(err) => Err(ExecutionError::from(err)),
        }
    }

    fn to_native(self: &Self, buf: &[u8]) -> Result<Operator, ExecutionError> {
        let decoded = self.deserialize(buf)?;
        decoded.convert_to_native()
    }

    fn convert_to_native(self: &Self) -> Result<Operator, ExecutionError> {
        match spark_operator::operator::OperatorType::from_i32(self.op_type) {
            Some(spark_operator::operator::OperatorType::Projection) => {
                match self.op_struct.as_ref().unwrap() {
                    spark_operator::operator::OpStruct::Projection(project) => {
                        let project_list = project
                            .project_list
                            .iter()
                            .map(|expr| expr.convert_to_native().unwrap())
                            .collect::<Vec<expression::Expr>>();

                        // We don't serialize leaf operator from Spark. Once there is empty child node for a serialized
                        // operator, it takes array batch from Spark.
                        // todo: put actual array batch.
                        let child = project
                            .child
                            .as_ref()
                            .map(|c| c.convert_to_native())
                            .unwrap_or_else(|| Ok(Operator::Scan(vec![])))
                            .unwrap();

                        Ok(Operator::Projection(project_list, Box::new(child)))
                    }
                }
            }
            None => Err(ExecutionError::NativeOpNotFound(self.op_type as i32)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::expression;
    use crate::functions;
    use crate::operators;
    use crate::serde::Serde;
    use crate::spark_expression;
    use crate::spark_operator;

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
    fn to_native_expr() {
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

    #[test]
    fn to_native_operator() {
        let mut op = spark_operator::Operator::default();
        op.set_op_type(spark_operator::operator::OperatorType::Projection);
        op.op_struct = Some(spark_operator::operator::OpStruct::Projection(Box::new(
            spark_operator::Projection {
                project_list: vec![],
                child: None,
            },
        )));

        let encoded = op.serialize();
        match op.to_native(&encoded.as_slice()).unwrap() {
            operators::Operator::Projection(project_list, child) => {
                assert_eq!(project_list.len(), 0);
                match child.as_ref() {
                    operators::Operator::Scan(batch) => assert!(batch.is_empty()),
                    _ => assert!(false, "wrong native operator!"),
                }
            }
            _ => assert!(false, "wrong native operator!"),
        }
    }
}
