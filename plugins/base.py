# Copyright (c) 2025 Ryan Lush <ryan.lush@gmail.com>
#
# Free for personal, educational, and open-source use.
# Commercial use requires written permission from the author.
# Contact: ryan.lush@gmail.com
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Ryan Lush <ryan.lush@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
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
