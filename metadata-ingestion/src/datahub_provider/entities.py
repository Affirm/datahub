from abc import abstractmethod

import attr

import datahub.emitter.mce_builder as builder
from datahub.utilities.urns.urn import guess_entity_type
from typing import ClassVar

class _Entity:
    @property
    @abstractmethod
    def urn(self) -> str:
        pass


@attr.s(auto_attribs=True, str=True)
class Dataset(_Entity):
    # This will help use jinja templates when setting these parameters.
    # Airflow Lineage Dataset entites support this
    # Eg: https://github.com/apache/airflow/blob/5b255dcab7f488601fd3d31e9be2ae70b7dea483/airflow/lineage/entities.py#LL30C4-L30C41
    template_fields: ClassVar = ("platform", "name", "env",)
    platform: str
    name: str
    env: str = builder.DEFAULT_ENV

    @property
    def urn(self):
        return builder.make_dataset_urn(self.platform, self.name, self.env)


@attr.s(str=True)
class Urn(_Entity):
    _urn: str = attr.ib()

    @_urn.validator
    def _validate_urn(self, attribute, value):
        if not value.startswith("urn:"):
            raise ValueError("invalid urn provided: urns must start with 'urn:'")
        if guess_entity_type(value) != "dataset":
            # This is because DataJobs only support Dataset lineage.
            raise ValueError("Airflow lineage currently only supports datasets")

    @property
    def urn(self):
        return self._urn
