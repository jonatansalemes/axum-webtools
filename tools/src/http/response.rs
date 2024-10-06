use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fmt;
use std::fmt::{Debug, Formatter};
use thiserror::Error;
use validator::{ValidationError, ValidationErrors};

#[derive(Debug)]
pub struct HttpErrorDetails {
    pub message: String,
    pub status_code: StatusCode,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Error, Display)]
pub enum HttpError {
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
    WithDetails(HttpErrorDetails),
    ValidationError(ValidationErrorResponse),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ValidationErrorResponse {
    pub validation_errors: Vec<ValidationError>,
}

impl ValidationErrorResponse {
    pub fn from(validation_errors: ValidationErrors) -> ValidationErrorResponse {
        let validation_errors = validation_errors
            .field_errors()
            .into_values()
            .flat_map(|v| v.clone())
            .collect();

        ValidationErrorResponse { validation_errors }
    }
}

impl Display for ValidationErrorResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.validation_errors)
    }
}

impl From<ValidationErrors> for HttpError {
    fn from(validation_errors: ValidationErrors) -> Self {
        HttpError::ValidationError(ValidationErrorResponse::from(validation_errors))
    }
}

impl Display for HttpErrorDetails {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let headers = self
            .headers
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect::<Vec<String>>()
            .join(", ");
        write!(
            f,
            "{:?}: {:?} ({:?})",
            self.status_code, self.message, headers
        )
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> axum::response::Response {
        let (status_code, message) = match self {
            Self::SqlxError(sqlx_error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("{:?}", sqlx_error),
            ),
            Self::WithDetails(details) => (details.status_code, details.message),
            Self::ValidationError(validation_error_response) => (
                StatusCode::BAD_REQUEST,
                format!("{:?}", validation_error_response),
            ),
        };

        let body = Json(json!({
            "message": message
        }));
        (status_code, body).into_response()
    }
}

macro_rules! http_error {
    ($name:ident,$status_code:expr) => {
        #[allow(missing_docs, unused)]
        pub fn $name<T>(message: impl Into<String>) -> Result<T, HttpError> {
            Err(HttpError::WithDetails(HttpErrorDetails {
                message: message.into(),
                status_code: $status_code,
                headers: vec![],
            }))
        }
    };
}

http_error!(conflict, StatusCode::CONFLICT);

http_error!(unauthorized, StatusCode::UNAUTHORIZED);

http_error!(bad_request, StatusCode::BAD_REQUEST);

http_error!(not_found, StatusCode::NOT_FOUND);

http_error!(internal_server_error, StatusCode::INTERNAL_SERVER_ERROR);

macro_rules! http_response {
    ($name:ident,$status:expr) => {
        #[allow(non_snake_case, missing_docs)]
        pub fn $name(
            value: impl Serialize + 'static,
        ) -> Result<axum::response::Response, HttpError> {
            Ok(($status, Json(value)).into_response())
        }
    };
}

http_response!(ok, StatusCode::OK);
