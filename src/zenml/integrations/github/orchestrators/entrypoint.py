# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Main entrypoint for containers with Kubeflow TFX component executors."""
import importlib
import logging
import sys
from typing import Optional, Type, cast

import click
from google.protobuf import json_format
from tfx.dsl.compiler import constants
from tfx.orchestration import metadata
from tfx.orchestration.local import runner_utils
from tfx.orchestration.portable import launcher
from tfx.proto.orchestration import pipeline_pb2

from zenml.artifacts.base_artifact import BaseArtifact
from zenml.artifacts.type_registry import type_registry
from zenml.integrations.registry import integration_registry
from zenml.orchestrators.utils import execute_step
from zenml.repository import Repository
from zenml.steps import BaseStep
from zenml.steps.utils import _FunctionExecutor, generate_component_class
from zenml.utils import source_utils


def _get_pipeline_node(
    pipeline: pipeline_pb2.Pipeline, node_id: str
) -> pipeline_pb2.PipelineNode:
    """Gets node of a certain node_id from a pipeline."""
    result: Optional[pipeline_pb2.PipelineNode] = None
    for node in pipeline.nodes:
        if (
            node.WhichOneof("node") == "pipeline_node"
            and node.pipeline_node.node_info.id == node_id
        ):
            result = node.pipeline_node
    if not result:
        logging.error("pipeline ir = %s\n", pipeline)
        raise RuntimeError(
            f"Cannot find node with id {node_id} in pipeline ir."
        )

    return result


def create_executor_class(
    step_source_path: str,
    executor_class_target_module_name: str,
) -> Type[_FunctionExecutor]:
    """Creates an executor class for a given step.

    Args:
        step_source_path: Import path of the step to run.
    """
    step_class = cast(
        Type[BaseStep], source_utils.load_source_path_class(step_source_path)
    )
    step_instance = step_class()

    materializers = step_instance.get_materializers(ensure_complete=True)

    # We don't publish anything to the metadata store inside this environment,
    # so the specific artifact classes don't matter
    input_spec = {}
    for key, value in step_class.INPUT_SIGNATURE.items():
        input_spec[key] = BaseArtifact

    output_spec = {}
    for key, value in step_class.OUTPUT_SIGNATURE.items():
        output_spec[key] = type_registry.get_artifact_type(value)[0]

    execution_parameters = {
        **step_instance.PARAM_SPEC,
        **step_instance._internal_execution_parameters,
    }

    component_class = generate_component_class(
        step_name=step_instance.name,
        step_module=executor_class_target_module_name,
        input_spec=input_spec,
        output_spec=output_spec,
        execution_parameter_names=set(execution_parameters),
        step_function=step_instance.entrypoint,
        materializers=materializers,
    )

    return cast(
        Type[_FunctionExecutor], component_class.EXECUTOR_SPEC.executor_class
    )


@click.command()
@click.option("--main_module", required=True, type=str)
@click.option("--step_source_path", required=True, type=str)
def main(
    main_module: str,
    step_source_path: str,
) -> None:
    """Runs a single ZenML step."""
    # prevent running entire pipeline in user code if they would run at import
    # time (e.g. not wrapped in a function or __name__=="__main__" check)
    constants.SHOULD_PREVENT_PIPELINE_EXECUTION = True

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)

    pb2_pipeline = pipeline_pb2.Pipeline()
    with open("tfx_ir.json", "r") as f:
        json_format.Parse(f.read(), pb2_pipeline)

    _, node_id = step_source_path.rsplit(".", maxsplit=1)
    pipeline_node = _get_pipeline_node(pb2_pipeline, node_id)

    deployment_config = runner_utils.extract_local_deployment_config(
        pb2_pipeline
    )
    executor_spec = runner_utils.extract_executor_spec(
        deployment_config, node_id
    )
    custom_driver_spec = runner_utils.extract_custom_driver_spec(
        deployment_config, node_id
    )

    # activate integrations and import the user main module to register all
    # materializers and stack components
    integration_registry.activate_integrations()
    importlib.import_module(main_module)

    if hasattr(executor_spec, "class_path"):
        executor_class_target_module_name, _ = getattr(
            executor_spec, "class_path"
        ).rsplit(".", maxsplit=1)
    else:
        raise RuntimeError(
            f"No class path found inside executor spec: {executor_spec}."
        )

    create_executor_class(
        step_source_path=step_source_path,
        executor_class_target_module_name=executor_class_target_module_name,
    )

    repo = Repository()
    metadata_store = repo.active_stack.metadata_store
    metadata_connection = metadata.Metadata(
        metadata_store.get_tfx_metadata_config()
    )

    # custom_executor_operators = {
    #     executable_spec_pb2.PythonClassExecutableSpec: step_instance.executor_operator
    # }

    component_launcher = launcher.Launcher(
        pipeline_node=pipeline_node,
        mlmd_connection=metadata_connection,
        pipeline_info=pb2_pipeline.pipeline_info,
        pipeline_runtime_spec=pb2_pipeline.runtime_spec,
        executor_spec=executor_spec,
        custom_driver_spec=custom_driver_spec,
        # custom_executor_operators=custom_executor_operators,
    )

    repo.active_stack.prepare_step_run()
    execute_step(component_launcher)
    repo.active_stack.cleanup_step_run()


if __name__ == "__main__":
    main()
