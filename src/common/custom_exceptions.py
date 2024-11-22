"""Custom Exceptions"""

class WrongFormatException(Exception):
    def __init__(self, message="Incorrect format"):
        super().__init__(message)
