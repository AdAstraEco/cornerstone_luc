import dataclasses
import functools
import json
import logging
import os
import typing

import dotenv

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Config:
    export_root: str
    harvard_dataverse_guestbook_json: str
    ingest_root: str
    number_of_dask_workers: int
    scratch_root: str
    usda_nass_api_key: str

    @classmethod
    @functools.cache
    def from_dot_env(cls) -> typing.Self:
        """Each field from its upper-case process environment variable, else from ``.env``.

        The environment wins so a container or a Cloud Run service can set every field
        without a file; ``.env`` is the fallback for local work and is only required when
        some field is still unset after the environment has been read.
        """
        fields = dataclasses.fields(cls)
        types = typing.get_type_hints(cls)
        values = {
            field.name: os.environ[field.name.upper()]
            for field in fields
            if field.name.upper() in os.environ
        }
        missing = [f.name for f in fields if f.name not in values]
        if missing:
            logger.info("Searching for a .env file")
            path_to_env = dotenv.find_dotenv()
            if path_to_env:
                logger.info(f"Loading configs from {path_to_env=:s}")
                configs = dotenv.dotenv_values(path_to_env)
                for name in missing:
                    if (value := configs.get(name.upper())) is not None:
                        values[name] = value
            unresolved = [n.upper() for n in missing if n not in values]
            if unresolved:
                raise OSError(
                    f"{unresolved} are set neither in the environment nor in a .env file"
                    f" ({path_to_env or 'none found'})"
                )
        return cls(**{name: types[name](value) for name, value in values.items()})

    def get_json_as_object(self, attr: str) -> dict[str, str]:
        s = getattr(self, attr)
        assert isinstance(s, str)
        return json.loads(s=s)
