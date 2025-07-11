"""Custom exceptions for the workerfacing API."""


class JobDeletedException(Exception):
    """Exception raised when a job has been deleted by the user."""

    pass


class JobNotAssignedException(Exception):
    """Exception raised when a job is not assigned to the current user."""

    pass
