# Axum Web Tools Macros

General purpose macros for axum web framework.

## Usage example

* `unit_test` macro

```rust

#[cfg(test)]
mod tests {
    
    use axum_webtools_macros::unit_test;

    #[unit_test]
    // your test will be wrapped in a block with dotenv::dotenv().ok();
    async fn test_function() {
        // your test code here
    }
}


```