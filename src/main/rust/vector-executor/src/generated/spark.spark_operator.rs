/// The basic message representing a Spark operator.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Operator {
    #[prost(enumeration="operator::OperatorType", tag="1")]
    pub op_type: i32,
    #[prost(oneof="operator::OpStruct", tags="2")]
    pub op_struct: ::core::option::Option<operator::OpStruct>,
}
/// Nested message and enum types in `Operator`.
pub mod operator {
    /// What the operator is actually.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum OperatorType {
        /// More operator types...
        Projection = 0,
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum OpStruct {
        #[prost(message, tag="2")]
        Projection(::prost::alloc::boxed::Box<super::Projection>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Projection {
    #[prost(message, repeated, tag="1")]
    pub project_list: ::prost::alloc::vec::Vec<super::spark_expression::Expr>,
    #[prost(message, optional, boxed, tag="2")]
    pub child: ::core::option::Option<::prost::alloc::boxed::Box<Operator>>,
}
