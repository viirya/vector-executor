//! Ser/De for expression/operators.

use prost::Message;
use std::fmt::Error;
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

/// A trait to convert a native representation to its protobuf one.
/// P is the protobuf type.
pub trait ToProto<P> {
    /// Convert to protobuf type P.
    fn to_proto(self: &Self) -> Result<P, Error>;
}

/// Convert expression::Expr to spark_expression::Expr.
impl ToProto<spark_expression::Expr> for expression::Expr {
    fn to_proto(self: &Self) -> Result<spark_expression::Expr, Error> {
        match self {
            expression::Expr::ScalarFunction { func, args } => {
                let proto_args = args
                    .into_iter()
                    .map(|e| e.to_proto().unwrap())
                    .collect::<Vec<_>>();
                match func {
                    functions::BuiltinScalarFunction::Add => {
                        let add_struct = spark_expression::expr::ExprStruct::Add(Box::new(
                            spark_expression::Add {
                                left: Some(Box::new(proto_args.get(0).unwrap().clone())),
                                right: Some(Box::new(proto_args.get(1).unwrap().clone())),
                            },
                        ));

                        Ok(spark_expression::Expr {
                            expr_struct: Some(add_struct),
                        })
                    }
                }
            }
            _ => Ok(spark_expression::Expr::default()),
        }
    }
}

/// Deserialize bytes to protobuf type of expression
pub fn deserialize_expr(buf: &[u8]) -> Result<spark_expression::Expr, expression::ExpressionError> {
    match spark_expression::Expr::decode(&mut Cursor::new(buf)) {
        Ok(e) => Ok(e),
        Err(err) => Err(expression::ExpressionError::from(err)),
    }
}

/// Deserialize bytes to protobuf type of operator
pub fn deserialize_op(buf: &[u8]) -> Result<spark_operator::Operator, ExecutionError> {
    match spark_operator::Operator::decode(&mut Cursor::new(buf)) {
        Ok(e) => Ok(e),
        Err(err) => Err(ExecutionError::from(err)),
    }
}

/// Trait for serialization/deserialization
/// E is error type, N is native type
pub trait Serde<E, N>
where
    Self: Sized,
{
    /// Serialize intermediate object to bytes
    /// For test only as we don't actually need to serialize objects from native to Scala/Java
    fn serialize(self: &Self) -> Vec<u8>;

    /// Deserilize bytes to native object, e.g. expression, operator
    fn to_native(self: &Self) -> Result<N, E>;
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

    fn to_native(self: &Self) -> Result<expression::Expr, expression::ExpressionError> {
        match self.expr_struct.as_ref().unwrap() {
            spark_expression::expr::ExprStruct::Add(add) => {
                let left = add.left.as_ref().unwrap().to_native()?;
                let right = add.right.as_ref().unwrap().to_native()?;

                Ok(functions::add(left.clone(), right.clone()))
            }
            spark_expression::expr::ExprStruct::Literal(spark_expression::Literal { value }) => {
                match value {
                    Some(spark_expression::literal::Value::IntVal(i)) => {
                        Ok(expression::Expr::Literal(
                            expression::ColumnarValue::Scalar(expression::LiteralValue::Int32(*i)),
                        ))
                    }
                    Some(spark_expression::literal::Value::LongVal(l)) => {
                        Ok(expression::Expr::Literal(
                            expression::ColumnarValue::Scalar(expression::LiteralValue::Int64(*l)),
                        ))
                    }
                    Some(spark_expression::literal::Value::FloatVal(f)) => {
                        Ok(expression::Expr::Literal(
                            expression::ColumnarValue::Scalar(expression::LiteralValue::Float(*f)),
                        ))
                    }
                    Some(spark_expression::literal::Value::DoubleVal(d)) => {
                        Ok(expression::Expr::Literal(
                            expression::ColumnarValue::Scalar(expression::LiteralValue::Double(*d)),
                        ))
                    }
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
                    Some(spark_expression::literal::Value::BoolVal(b)) => {
                        Ok(expression::Expr::Literal(
                            expression::ColumnarValue::Scalar(expression::LiteralValue::Bool(*b)),
                        ))
                    }
                    None => Err(expression::ExpressionError::GeneralError(format!(
                        "Literal message type shouldn't have empty value!"
                    ))),
                }
            }
            spark_expression::expr::ExprStruct::Bound(bound) => {
                Ok(expression::Expr::BoundReference(bound.index as u32))
            }
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

    fn to_native(self: &Self) -> Result<Operator, ExecutionError> {
        match self.op_struct.as_ref().unwrap() {
            spark_operator::operator::OpStruct::Projection(project) => {
                let mut project_list = vec![];
                for serialized_expr in &project.project_list {
                    match serialized_expr.to_native() {
                        Ok(expr) => project_list.push(expr),
                        Err(err) => return Err(From::from(err)),
                    }
                }

                // We don't serialize leaf operator from Spark. Once there is empty child node for a serialized
                // operator, it takes array batch from Spark.
                // todo: put actual array batch.
                let child = project
                    .child
                    .as_ref()
                    .map(|c| c.to_native())
                    .unwrap_or_else(|| Ok(Operator::Scan(vec![])))
                    .unwrap();

                Ok(Operator::Projection(project_list, Box::new(child)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::expression;
    use crate::functions;
    use crate::operators;
    use crate::serde::{deserialize_expr, deserialize_op, Serde};
    use crate::spark_expression;
    use crate::spark_operator;

    #[test]
    fn ser_de() {
        let expr = spark_expression::Expr::default();

        let encoded = expr.serialize();
        let decoded = deserialize_expr(&encoded.as_slice());

        assert_eq!(expr, decoded.unwrap())
    }

    #[test]
    fn to_native_expr() {
        let mut expr = spark_expression::Expr::default();

        let mut literal1 = spark_expression::Expr::default();
        literal1.expr_struct = Some(spark_expression::expr::ExprStruct::Literal(
            spark_expression::Literal {
                value: Some(spark_expression::literal::Value::IntVal(1)),
            },
        ));
        let mut literal2 = spark_expression::Expr::default();
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
        match deserialize_expr(&encoded.as_slice())
            .unwrap()
            .to_native()
            .unwrap()
        {
            expression::Expr::ScalarFunction { func, .. } => {
                assert_eq!(func, functions::BuiltinScalarFunction::Add)
            }
            _ => assert!(false, "wrong native expression!"),
        }
    }

    #[test]
    fn to_native_operator() {
        let mut op = spark_operator::Operator::default();
        op.op_struct = Some(spark_operator::operator::OpStruct::Projection(Box::new(
            spark_operator::Projection {
                project_list: vec![],
                child: None,
            },
        )));

        let encoded = op.serialize();
        match deserialize_op(&encoded.as_slice())
            .unwrap()
            .to_native()
            .unwrap()
        {
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
