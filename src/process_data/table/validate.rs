//! Field validation and type checking
//!
//! This module handles validation and type coercion for document fields.

use crate::{Error, ErrorDetail, process_data::ColumnValue, schema};

use super::context::RecordContext;

macro_rules! bail {
    ($ctx:expr, $detail:expr) => {
        return Err($ctx.error($detail))
    };
}

/// Check if a field is normally required (not an ID or Hash field).
pub fn is_normal_required_field(def: &schema::FieldType) -> bool {
    match def {
        schema::FieldType::Id => false,
        schema::FieldType::Hash => false,
        schema::FieldType::String { required, .. } => *required,
        schema::FieldType::Boolean { required, .. } => *required,
        schema::FieldType::Integer { required, .. } => *required,
        schema::FieldType::Real { required, .. } => *required,
        schema::FieldType::Date { required, .. } => *required,
        schema::FieldType::Datetime { required, .. } => *required,
        schema::FieldType::Image { required, .. } => *required,
        schema::FieldType::File { required, .. } => *required,
        schema::FieldType::Markdown { required, .. } => *required,
        schema::FieldType::Records { required, .. } => *required,
    }
}

pub fn process_hash_field(ctx: &RecordContext, name: &str) -> Result<ColumnValue, Error> {
    bail!(ctx.error, ErrorDetail::FoundComputedField(name.to_owned()))
}

pub fn process_boolean_field(
    ctx: &RecordContext,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::Bool(b) = value {
        Ok(ColumnValue::Boolean(b))
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "boolean",
                got: value,
            }
        );
    }
}

pub fn process_integer_field(
    ctx: &RecordContext,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::Number(n) = value {
        if n.is_i64() {
            Ok(ColumnValue::Number(n))
        } else {
            bail!(
                &ctx.error,
                ErrorDetail::TypeMismatch {
                    expected: "integer",
                    got: n.into(),
                }
            );
        }
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "integer",
                got: value,
            }
        );
    }
}

pub fn process_real_field(
    ctx: &RecordContext,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::Number(n) = value {
        if n.is_f64() {
            Ok(ColumnValue::Number(n))
        } else {
            bail!(
                &ctx.error,
                ErrorDetail::TypeMismatch {
                    expected: "real",
                    got: n.into(),
                }
            );
        }
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "real",
                got: value,
            }
        );
    }
}

pub fn process_string_field(
    ctx: &RecordContext,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::String(string) = value {
        Ok(ColumnValue::String(string))
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "string",
                got: value,
            }
        );
    }
}

pub fn process_date_field(
    ctx: &RecordContext,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::String(date) = value {
        let date = date
            .parse::<chrono::NaiveDate>()
            .map_err(|_| ctx.error.error(ErrorDetail::InvalidDate(date.to_owned())))?;
        Ok(ColumnValue::Date(date))
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "date",
                got: value,
            }
        );
    }
}

pub fn process_datetime_field(
    ctx: &RecordContext,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::String(datetime) = value {
        let datetime = datetime.parse::<chrono::NaiveDateTime>().map_err(|_| {
            ctx.error
                .error(ErrorDetail::InvalidDatetime(datetime.to_owned()))
        })?;
        Ok(ColumnValue::Datetime(datetime))
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "datetime",
                got: value,
            }
        );
    }
}
