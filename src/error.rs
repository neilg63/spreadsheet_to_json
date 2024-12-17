use std::error::Error;
use std::fmt;


/// Simple GenericError type to cover other error type with different implementations
#[derive(Debug)]
pub struct GenericError(pub &'static str);

impl fmt::Display for GenericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for GenericError {}

/// convert IO Error to GenericError
impl From<calamine::Error> for GenericError {
  fn from(error: calamine::Error) -> Self {
    match error {
      calamine::Error::Io(_) => GenericError("io_error"),
      calamine::Error::Xlsx(_) => GenericError("xlsx_error"),
      calamine::Error::Xls(_) => GenericError("xlsx_error"),
      calamine::Error::Ods(_) => GenericError("ods_error"),
      // Add more patterns as needed for other calamine::Error variants
      _ => GenericError("unknown_calamine_error"),
    }
  }
}

/// convert IO Error to GenericError
impl From<std::io::Error> for GenericError {
  fn from(error: std::io::Error) -> Self {
    match error.kind() {
      std::io::ErrorKind::NotFound => GenericError("file_not_found"),
      std::io::ErrorKind::PermissionDenied => GenericError("permission_denied"),
      std::io::ErrorKind::ConnectionRefused => GenericError("connection_refused"),
      _ => GenericError("io_error"),
    }
  }
}

