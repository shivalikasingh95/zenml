#  Copyright (c) ZenML GmbH 2022. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class PipelineName(BaseModel):
    """A named pipeline whose implementation may change over time.

    Attributes:
         id: unique id
         name: the name of the pipeline.
         create_time: time that the pipeline was first registered
    """

    id: UUID = Field(default_factory=uuid4)
    name: str
    create_time: datetime


class PipelineVersion(BaseModel):
    """A snapshot of a single implementation of a PipelineName.

    Attributes:
         id: unique id
         pipeline_id: id of the PipelineName this belongs to
         timestamp: time that this version was first registered
    """

    id: UUID = Field(default_factory=uuid4)
    pipeline_id: UUID
    timestamp: datetime
