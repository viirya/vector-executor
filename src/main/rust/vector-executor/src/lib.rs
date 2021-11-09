//! PoC of vectorization execution through JNI to Rust.
#![deny(warnings)]
#![deny(missing_docs)]

pub mod jni_api;

pub mod datatype;
pub mod expression;
pub mod functions;
pub mod operators;
pub mod serde;

// Include generated modules from .proto files.
#[allow(missing_docs)]
pub mod spark_expression {
    include!(concat!("generated", "/spark.spark_expression.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
pub mod spark_operator {
    include!(concat!("generated", "/spark.spark_operator.rs"));
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
