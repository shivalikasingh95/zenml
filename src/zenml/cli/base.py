#  Copyright (c) ZenML GmbH 2020. All Rights Reserved.
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

from pathlib import Path
from typing import Optional

import click

from zenml.cli.cli import cli
from zenml.cli.utils import confirmation, declare, error, warning
from zenml.config.global_config import GlobalConfiguration
from zenml.console import console
from zenml.constants import CONFIG_FILE_NAME, REPOSITORY_DIRECTORY_NAME
from zenml.exceptions import InitializationException
from zenml.io import fileio
from zenml.io.utils import get_global_config_directory
from zenml.repository import Repository
from zenml.utils import yaml_utils


@cli.command("init", help="Initialize a ZenML repository.")
@click.option(
    "--path",
    type=click.Path(
        exists=True, file_okay=False, dir_okay=True, path_type=Path
    ),
)
def init(path: Optional[Path]) -> None:
    """Initialize ZenML on given path.

    Args:
      path: Path to the repository.

    Raises:
        InitializationException: If the repo is already initialized.
    """
    if path is None:
        path = Path.cwd()

    with console.status(f"Initializing ZenML repository at {path}.\n"):
        try:
            Repository.initialize(root=path)
            declare(f"ZenML repository initialized at {path}.")
        except InitializationException as e:
            error(f"{e}")

    cfg = GlobalConfiguration()
    declare(
        f"The local active profile was initialized to "
        f"'{cfg.active_profile_name}' and the local active stack to "
        f"'{cfg.active_stack_name}'. This local configuration will only take "
        f"effect when you're running ZenML from the initialized repository "
        f"root, or from a subdirectory. For more information on profile "
        f"and stack configuration, please visit "
        f"https://docs.zenml.io."
    )


def _delete_local_artifact_metadata() -> None:
    """Delete local metadata and artifact stores from the active stack."""
    with console.status(
        "Deleting local artifact and metadata stores from active stack.\n"
    ):
        # TODO: [LOW] implement this
        # check that an active stack exists
        # check that the metadata store is local
        # check that the artifact store is local
        # get the path for them both
        # delete everything inside that path
        return


@cli.command("clean", hidden=True)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.",
)
@click.option(
    "--local",
    "-l",
    is_flag=True,
    default=False,
    help="Delete local metadata and artifact stores from the active stack.",
)
def clean(yes: bool = False, local: bool = False) -> None:
    """Delete all ZenML metadata, artifacts, profiles and stacks.

    This is a destructive operation, primarily intended for use in development.

    Args:
      yes: bool:  (Default value = False)
    """
    if local:
        _delete_local_artifact_metadata()
        return

    if not yes:
        confirm = confirmation(
            "DANGER: This will completely delete all artifacts, metadata, stacks and profiles ever created during the use of ZenML. Pipelines and stack components running non-locally will still exist. Please delete them manually. Are you sure you want to proceed?"
        )

    if yes or confirm:
        # delete the .zen folder
        local_zen_repo_config = Path.cwd() / REPOSITORY_DIRECTORY_NAME
        if fileio.exists(str(local_zen_repo_config)):
            fileio.rmtree(str(local_zen_repo_config))
            declare(f"Deleted local ZenML config from {local_zen_repo_config}.")

        # delete the `zen_examples` if they were pulled
        global_zen_config = Path(get_global_config_directory())
        zenml_examples_dir = global_zen_config / "zen_examples"
        if fileio.exists(str(zenml_examples_dir)):
            fileio.rmtree(str(zenml_examples_dir))
            declare(f"Deleted ZenML examples from {zenml_examples_dir}.")

        # delete the profiles (and stacks)
        if fileio.exists(str(global_zen_config)):
            config_yaml_path = global_zen_config / CONFIG_FILE_NAME
            config_yaml_data = yaml_utils.read_yaml(str(config_yaml_path))
            old_user_id = config_yaml_data["user_id"]
            for dir_name in fileio.listdir(str(global_zen_config)):
                if fileio.isdir(str(global_zen_config / str(dir_name))):
                    warning(
                        f"Deleting '{str(dir_name)}' directory from global config."
                    )
            fileio.rmtree(str(global_zen_config))
            declare(f"Deleted global ZenML config from {global_zen_config}.")

        Repository.initialize(root=Path.cwd())
        if old_user_id:
            new_config_yaml_data = yaml_utils.read_yaml(str(config_yaml_path))
            new_config_yaml_data["user_id"] = old_user_id
            yaml_utils.write_yaml(str(config_yaml_path), new_config_yaml_data)

        declare(f"Reinitialized ZenML global config at {Path.cwd()}.")

    else:
        declare("Aborting clean.")
