"""Stable command exit contract exceptions."""
class TransientFailure(RuntimeError):
    pass

class AssetFailure(RuntimeError):
    pass

class SilverDateFailure(RuntimeError):
    pass

class WeeklyGateFailureExit(RuntimeError):
    pass
