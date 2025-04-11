use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use derive_more::with_trait::Display;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use thiserror::Error;
use utoipa::ToSchema;
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
        let (status_code, body) = match self {
            Self::SqlxError(sqlx_error) => {
                let message = format!("{:?}", sqlx_error);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!(InternalServerErrorResponse {
                        message,
                        status_code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    }),
                )
            }
            Self::WithDetails(details) => {
                let message = details.message;
                let status_code_enum = details.status_code;
                let status_code = status_code_enum.as_u16();
                (
                    status_code_enum,
                    match status_code_enum {
                        StatusCode::CONFLICT => json!(ConflictResponse {
                            message,
                            status_code,
                        }),
                        StatusCode::UNAUTHORIZED => json!(UnauthorizedResponse {
                            message,
                            status_code,
                        }),
                        StatusCode::NOT_FOUND => json!(NotFoundResponse {
                            message,
                            status_code,
                        }),
                        StatusCode::INTERNAL_SERVER_ERROR => {
                            json!(InternalServerErrorResponse {
                                message,
                                status_code,
                            })
                        }
                        StatusCode::BAD_REQUEST => json!(BadRequestResponse {
                            message,
                            status_code,
                        }),
                        _ => json!(UnprocessableEntityResponse {
                            message,
                            status_code,
                        }),
                    },
                )
            }
            Self::ValidationError(validation_error_response) => {
                let messages = validation_error_response
                    .validation_errors
                    .iter()
                    .map(|error| {
                        let code = error.code.to_string();
                        let message = error.message.clone().map(|m| m.to_string());
                        let params = error
                            .params
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.clone()))
                            .collect::<HashMap<_, _>>();
                        BadRequestValidationErrorItemResponse {
                            code,
                            message,
                            params,
                        }
                    })
                    .collect::<Vec<_>>();
                let json_value = json!(BadRequestValidationErrorResponse {
                    messages,
                    status_code: StatusCode::BAD_REQUEST.as_u16(),
                });
                (StatusCode::BAD_REQUEST, json_value)
            }
        };

        (status_code, Json(body)).into_response()
    }
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct BadRequestValidationErrorItemResponse {
    pub code: String,
    pub message: Option<String>,
    pub params: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct BadRequestValidationErrorResponse {
    pub messages: Vec<BadRequestValidationErrorItemResponse>,
    pub status_code: u16,
}

macro_rules! http_error_struct {
    ($name:ident) => {
        #[derive(Serialize, Deserialize, ToSchema)]
        pub struct $name {
            pub message: String,
            #[serde(rename = "statusCode")]
            pub status_code: u16,
        }
    };
}

http_error_struct!(ConflictResponse);
http_error_struct!(NotFoundResponse);
http_error_struct!(InternalServerErrorResponse);
http_error_struct!(BadRequestResponse);
http_error_struct!(UnauthorizedResponse);
http_error_struct!(UnprocessableEntityResponse);

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

http_error!(unprocessable_entity, StatusCode::UNPROCESSABLE_ENTITY);

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
http_response!(created, StatusCode::CREATED);

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use validator::Validate;

    #[derive(Debug, Validate)]
    struct Test {
        #[validate(length(min = 5))]
        name: String,
    }

    #[tokio::test]
    async fn test_validation_error_into_response() {
        let test = Test {
            name: "test".to_string(),
        };

        let validation_errors = test.validate().unwrap_err();

        let http_error: HttpError = validation_errors.into();

        let response = http_error.into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            body,
            "{\"messages\":[{\"code\":\"length\",\"message\":null,\"params\":{\"min\":5,\"value\":\"test\"}}],\"status_code\":400}"
        );
    }
}
