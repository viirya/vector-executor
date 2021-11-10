/// The basic message representing a Spark expression.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Expr {
    #[prost(oneof="expr::ExprStruct", tags="2, 3, 4")]
    pub expr_struct: ::core::option::Option<expr::ExprStruct>,
}
/// Nested message and enum types in `Expr`.
pub mod expr {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ExprStruct {
        #[prost(message, tag="2")]
        Literal(super::Literal),
        #[prost(message, tag="3")]
        Add(::prost::alloc::boxed::Box<super::Add>),
        #[prost(message, tag="4")]
        Bound(super::BoundReference),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Literal {
    #[prost(oneof="literal::Value", tags="1, 2, 3, 4, 5, 6, 7")]
    pub value: ::core::option::Option<literal::Value>,
}
/// Nested message and enum types in `Literal`.
pub mod literal {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(bool, tag="1")]
        BoolVal(bool),
        #[prost(int32, tag="2")]
        IntVal(i32),
        #[prost(int64, tag="3")]
        LongVal(i64),
        #[prost(float, tag="4")]
        FloatVal(f32),
        #[prost(double, tag="5")]
        DoubleVal(f64),
        #[prost(string, tag="6")]
        StringVal(::prost::alloc::string::String),
        #[prost(bytes, tag="7")]
        BytesVal(::prost::alloc::vec::Vec<u8>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Add {
    #[prost(message, optional, boxed, tag="1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(message, optional, boxed, tag="2")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
/// Bound to a particular vector array in input batch.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BoundReference {
    #[prost(int32, tag="1")]
    pub index: i32,
}
