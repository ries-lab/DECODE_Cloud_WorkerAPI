"""Custom exceptions for the workerfacing API."""


class JobDeletedException(Exception):
    """Exception raised when a job has been deleted by the user."""
    
    def __init__(self, job_id: int, message: str = "Job was probably deleted by the user"):
        """Initialize the exception.
        
        Args:
            job_id: The ID of the job that was deleted
            message: The error message
        """
        self.job_id = job_id
        super().__init__(f"Job {job_id} not found; {message}")