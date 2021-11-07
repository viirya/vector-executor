/// The basic message representing a Spark expression.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Expr {
    #[prost(message, repeated, tag="1")]
    pub children: ::prost::alloc::vec::Vec<Expr>,
    #[prost(enumeration="expr::ExprType", tag="2")]
    pub expr_type: i32,
    #[prost(message, optional, tag="3")]
    pub options: ::core::option::Option<Options>,
}
/// Nested message and enum types in `Expr`.
pub mod expr {
    /// What the expression is actually.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ExprType {
        /// More expression types...
        Add = 0,
    }
}
/// Options of expressions. The actual options are depending on the expression type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Options {
}
