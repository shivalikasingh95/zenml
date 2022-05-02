#  Copyright (c) ZenML GmbH 2021. All Rights Reserved.
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
import json
import os
import sys
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Tuple

from google.protobuf import json_format
from tfx.dsl.compiler.compiler import Compiler
from tfx.dsl.compiler.constants import PIPELINE_RUN_ID_PARAMETER_NAME
from tfx.orchestration.portable import runtime_parameter_utils
from tfx.proto.orchestration import pipeline_pb2
from tfx.proto.orchestration.pipeline_pb2 import PipelineNode

import zenml
import zenml.constants
import zenml.io.utils
from zenml.enums import MetadataContextTypes
from zenml.integrations.constants import GITHUB
from zenml.logger import get_logger
from zenml.orchestrators import BaseOrchestrator, context_utils
from zenml.repository import Repository
from zenml.stack import Stack
from zenml.stack.stack_component_class_registry import (
    register_stack_component_class,
)
from zenml.utils import source_utils
from zenml.utils.source_utils import get_source_root_path
from zenml.utils.yaml_utils import write_yaml

if TYPE_CHECKING:
    from zenml.pipelines.base_pipeline import BasePipeline
    from zenml.runtime_configuration import RuntimeConfiguration

logger = get_logger(__name__)
from git.repo.base import Repo

from zenml.io.utils import write_file_contents_as_string
from zenml.orchestrators.utils import create_tfx_pipeline

BASE_ENTRYPOINT_COMMAND = (
    "python -m zenml.integrations.github.orchestrators.entrypoint"
)

# TODO: validate: inside git repo, github remote, github container registry, non-local artifact/metadata store


@register_stack_component_class
class GithubOrchestrator(BaseOrchestrator):
    """Orchestrator responsible for running pipelines using GitHub workflows."""

    _repo: Repo
    custom_docker_base_image_name: Optional[str] = None

    # Class Configuration
    FLAVOR: ClassVar[str] = GITHUB

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._repo = Repo(search_parent_directories=True)

    @property
    def git_repo_root(self) -> str:
        return self._repo.working_tree_dir

    @property
    def github_workflow_dir(self) -> str:
        return os.path.join(self.git_repo_root, ".github", "workflows")

    def get_docker_image_name(self, pipeline_name: str) -> str:
        """Returns the full docker image name including registry and tag."""

        base_image_name = f"zenml-github:{pipeline_name}"
        container_registry = Repository().active_stack.container_registry

        if container_registry:
            registry_uri = container_registry.uri.rstrip("/")
            return f"{registry_uri}/{base_image_name}"
        else:
            return base_image_name

    def _configure_pb2_pipeline(
        self,
        pipeline: "BasePipeline",
        pb2_pipeline: pipeline_pb2.Pipeline,
        stack: "Stack",
        runtime_configuration: "RuntimeConfiguration",
    ):
        runtime_parameter_utils.substitute_runtime_parameter(
            pb2_pipeline,
            {
                PIPELINE_RUN_ID_PARAMETER_NAME: runtime_configuration.run_name,
            },
        )

        for node in pb2_pipeline.nodes:
            pipeline_node: PipelineNode = node.pipeline_node

            # fill out that context
            context_utils.add_context_to_node(
                pipeline_node,
                type_=MetadataContextTypes.STACK.value,
                name=str(hash(json.dumps(stack.dict(), sort_keys=True))),
                properties=stack.dict(),
            )

            # Add all pydantic objects from runtime_configuration to the context
            context_utils.add_runtime_configuration_to_node(
                pipeline_node, runtime_configuration
            )

            # Add pipeline requirements as a context
            requirements = " ".join(sorted(pipeline.requirements))
            context_utils.add_context_to_node(
                pipeline_node,
                type_=MetadataContextTypes.PIPELINE_REQUIREMENTS.value,
                name=str(hash(requirements)),
                properties={"pipeline_requirements": requirements},
            )

    def prepare_pipeline_deployment(
        self,
        pipeline: "BasePipeline",
        stack: "Stack",
        runtime_configuration: "RuntimeConfiguration",
    ) -> None:
        """Builds a docker image for the current environment and uploads it to
        a container registry if configured."""
        from zenml.utils.docker_utils import (
            build_docker_image,
            push_docker_image,
        )

        container_registry = stack.container_registry
        assert container_registry

        image_name = self.get_docker_image_name(pipeline.name)
        requirements = {*stack.requirements(), *pipeline.requirements}
        logger.debug("Github docker container requirements: %s", requirements)

        build_context_path = get_source_root_path()
        tfx_ir_path = os.path.join(build_context_path, "tfx_ir.json")

        tfx_pipeline = create_tfx_pipeline(pipeline, stack=stack)
        pb2_pipeline = Compiler().compile(tfx_pipeline)
        self._configure_pb2_pipeline(
            pipeline=pipeline,
            pb2_pipeline=pb2_pipeline,
            runtime_configuration=runtime_configuration,
            stack=stack,
        )

        write_file_contents_as_string(
            tfx_ir_path, json_format.MessageToJson(pb2_pipeline)
        )

        try:
            build_docker_image(
                build_context_path=build_context_path,
                image_name=image_name,
                dockerignore_path=pipeline.dockerignore_file,
                requirements=requirements,
                base_image=self.custom_docker_base_image_name,
            )
        finally:
            # make sure the tfx ir gets deleted
            os.remove(tfx_ir_path)

        push_docker_image(image_name)

        jobs = {}
        for component in tfx_pipeline.components:
            main_module, step_source_path = self._resolve_user_modules(
                component.type
            )
            upstream_steps = [node.id for node in component.upstream_nodes]
            entrypoint_command = " ".join(
                [
                    "docker",
                    "run",
                    image_name,
                    BASE_ENTRYPOINT_COMMAND,
                    "--main_module",
                    main_module,
                    "--step_source_path",
                    step_source_path,
                ]
            )

            step_dict = self._create_step_dict(
                step_name=component.id,
                container_registry_uri=container_registry.uri,
                entrypoint_command=entrypoint_command,
                upstream_steps=upstream_steps,
            )
            jobs[component.id] = step_dict

        pipeline_dict = self._create_pipeline_dict(
            pipeline_name=pipeline.name, jobs=jobs
        )

        workflow_path = os.path.join(
            self.github_workflow_dir, f"{pipeline.name}.yml"
        )
        write_yaml(workflow_path, pipeline_dict, sort_keys=False)

    def _create_step_dict(
        self,
        step_name: str,
        container_registry_uri: str,
        upstream_steps: List[str],
        entrypoint_command: str,
    ) -> Dict[str, Any]:
        steps = [
            {
                "uses": "docker/login-action@v1",
                "with": {
                    "registry": container_registry_uri,
                    "username": "${{ github.actor }}",
                    "password": "${{ secrets.github_token }}",
                },
            },
            {"run": entrypoint_command},
        ]
        return {
            "name": f"Run step {step_name}",
            "runs-on": "ubuntu-latest",
            "needs": upstream_steps,
            "steps": steps,
        }

    def _create_pipeline_dict(
        self, pipeline_name: str, jobs: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {
            "name": f"Run pipeline {pipeline_name}",
            "on": ["workflow_dispatch", "push"],
            "jobs": jobs,
        }

    @staticmethod
    def _resolve_user_modules(component_type: str) -> Tuple[str, str]:
        """Resolves the main and step module.

        Args:
            component_type: Component type.

        Returns:
            A tuple containing the path of the resolved main module and step
            class.
        """
        main_module_path = zenml.constants.USER_MAIN_MODULE
        if not main_module_path:
            main_module_path = source_utils.get_module_source_from_module(
                sys.modules["__main__"]
            )

        step_module_path, step_class = component_type.rsplit(".", maxsplit=1)
        if step_module_path == "__main__":
            step_module_path = main_module_path

        step_source_path = f"{step_module_path}.{step_class}"

        return main_module_path, step_source_path

    def run_pipeline(
        self,
        pipeline: "BasePipeline",
        stack: "Stack",
        runtime_configuration: "RuntimeConfiguration",
    ) -> Any:
        workflow_path = os.path.join(
            self.github_workflow_dir, f"{pipeline.name}.yml"
        )
        self._repo.index.add(workflow_path)
        self._repo.index.commit(f"[ZenML Github orchestrator] Added {pipeline.name} github workflow.")
        self._repo.remote().push()
