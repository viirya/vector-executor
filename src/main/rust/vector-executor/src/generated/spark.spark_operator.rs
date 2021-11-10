/// The basic message representing a Spark operator.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Operator {
    #[prost(oneof="operator::OpStruct", tags="2")]
    pub op_struct: ::core::option::Option<operator::OpStruct>,
}
/// Nested message and enum types in `Operator`.
pub mod operator {
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
