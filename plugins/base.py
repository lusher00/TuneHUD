"""
tunehud_gateway/plugins/base.py
Base class for all TuneHUD transport plugins. Python 3.7 compatible.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional, List, Tuple


@dataclass
class ParamDescriptor:
    name:        str
    label:       str
    type:        str
    units:       Optional[str] = None
    min:         Optional[float] = None
    max:         Optional[float] = None
    step:        Optional[float] = None
    read_only:   bool = False
    group:       Optional[str] = None
    enum_values: Optional[list] = None
    description: Optional[str] = None

    def to_dict(self):
        return {
            'name': self.name, 'label': self.label, 'type': self.type,
            'units': self.units, 'min': self.min, 'max': self.max,
            'step': self.step, 'read_only': self.read_only,
            'group': self.group, 'enum_values': self.enum_values,
            'description': self.description,
        }

    def validate(self, value):
        if self.read_only:
            return False, '{} is read-only'.format(self.name)
        try:
            if self.type == 'float':
                v = float(value)
            elif self.type == 'int':
                v = int(value)
            elif self.type == 'bool':
                return True, ''
            elif self.type == 'enum':
                if self.enum_values:
                    valid = [e['value'] for e in self.enum_values]
                    if value not in valid:
                        return False, '{} not in {}'.format(value, valid)
                return True, ''
            else:
                v = float(value)
        except (ValueError, TypeError) as e:
            return False, str(e)
        if self.min is not None and v < self.min:
            return False, '{} < min {}'.format(v, self.min)
        if self.max is not None and v > self.max:
            return False, '{} > max {}'.format(v, self.max)
        return True, ''


class TransportPlugin(ABC):
    def __init__(self, config):
        self.config = config
        self._manifest = None

    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def disconnect(self):
        pass

    @abstractmethod
    async def get_manifest(self):
        pass

    @abstractmethod
    async def read_param(self, name):
        pass

    @abstractmethod
    async def write_param(self, name, value):
        pass

    @abstractmethod
    async def read_all(self):
        pass

    @property
    def name(self):
        return self.config.get('name', self.__class__.__name__)

    def is_connected(self):
        return False
