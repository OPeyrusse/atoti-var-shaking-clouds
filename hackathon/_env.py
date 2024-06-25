import os
from functools import cached_property

_PRIVATE_KEY_ID = "PRIVATE_KEY_ID"
_PRIVATE_KEY = "PRIVATE_KEY"


class _Environment:
    @staticmethod
    def get_env_or_throw(env_var: str) -> str:
        value = os.environ.get(env_var)
        if value is None:
            raise ValueError(f"Missing environment variable: {env_var}")
        return value

    # @cached_property
    # def private_key_id(self):
    #     return self.get_env_or_throw(_PRIVATE_KEY_ID)

    # @cached_property
    # def private_key(self):
    #     return self.get_env_or_throw(_PRIVATE_KEY)


env = _Environment()